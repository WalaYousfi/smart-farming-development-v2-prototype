# Role: Find a historical Gold run, repeat its processing, and compare the new anomaly predictions 
# and scores with the originals.

import argparse
import json
import subprocess
import sys

import numpy as np
import pandas as pd

from io import BytesIO

from pipeline.common.config import (
    MANIFEST_PREFIX,
    MINIO_BUCKET,
)
from pipeline.common.minio_client import create_minio_client
from pipeline.gold.integration_run_selector import read_json_object


JOB_NAME = "gold_integrated_field_anomaly_detection"


def manifest_path(run_id):
    return (
        "{}/{}/run_id={}/manifest_completed.json"
        .format(MANIFEST_PREFIX, JOB_NAME, run_id)
    )


def load_manifest(client, run_id):
    manifest = read_json_object(
        client,
        manifest_path(run_id),
    )

    if manifest.get("status") != "completed":
        raise ValueError("Historical Gold run is not completed")

    return manifest


def load_parquet(client, object_name):
    response = client.get_object(
        MINIO_BUCKET,
        object_name,
    )

    try:
        return pd.read_parquet(
            BytesIO(response.read())
        )
    finally:
        response.close()
        response.release_conn()


def scored_object(manifest):
    matches = [
        name
        for name in manifest.get("output_objects", [])
        if name.endswith("scored_integrated_observations.parquet")
    ]

    if len(matches) != 1:
        raise ValueError(
            "Expected exactly one scored Gold Parquet object"
        )

    return matches[0]


def list_completed_run_ids(client):
    prefix = "{}/{}/".format(MANIFEST_PREFIX, JOB_NAME)

    return set(
        obj.object_name.split("run_id=", 1)[1].split("/", 1)[0]
        for obj in client.list_objects(
            MINIO_BUCKET,
            prefix=prefix,
            recursive=True,
        )
        if obj.object_name.endswith("manifest_completed.json")
        and "run_id=" in obj.object_name
    )


def compare_outputs(original, replayed):
    columns = [
        "anomaly_prediction",
        "anomaly_score",
    ]

    if len(original) != len(replayed):
        return {
            "same_record_count": False,
            "predictions_identical": False,
            "scores_close": False,
        }

    for column in columns:
        if column not in original or column not in replayed:
            raise ValueError("Missing comparison column: " + column)

    predictions_identical = np.array_equal(
        original["anomaly_prediction"].values,
        replayed["anomaly_prediction"].values,
    )

    scores_close = np.allclose(
        original["anomaly_score"].values,
        replayed["anomaly_score"].values,
        rtol=1e-8,
        atol=1e-10,
    )

    return {
        "same_record_count": True,
        "predictions_identical": bool(predictions_identical),
        "scores_close": bool(scores_close),
        "maximum_score_difference": float(
            np.max(
                np.abs(
                    original["anomaly_score"].values
                    - replayed["anomaly_score"].values
                )
            )
        ) if len(original) else 0.0,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-run-id", required=True)
    arguments = parser.parse_args()

    client = create_minio_client()

    historical = load_manifest(
        client,
        arguments.gold_run_id,
    )

    metrics = historical.get("metrics", {})

    integration_run_id = metrics.get("integration_run_id")
    contamination = metrics.get("contamination")

    if not integration_run_id or contamination is None:
        raise ValueError(
            "Historical manifest lacks replay configuration"
        )

    before = list_completed_run_ids(client)

    subprocess.check_call([
        sys.executable,
        "-m",
        "pipeline.gold.integrated_anomaly_detection",
        "--integration-run-id",
        str(integration_run_id),
        "--contamination",
        str(contamination),
    ])

    after = list_completed_run_ids(client)
    new_runs = after - before

    if len(new_runs) != 1:
        raise RuntimeError(
            "Could not uniquely identify the replayed Gold run"
        )

    replay_run_id = new_runs.pop()
    replay_manifest = load_manifest(client, replay_run_id)

    if (
        historical.get("input_objects")
        != replay_manifest.get("input_objects")
    ):
        raise RuntimeError(
            "Replay did not select the original input object paths"
        )

    original_data = load_parquet(
        client,
        scored_object(historical),
    )

    replay_data = load_parquet(
        client,
        scored_object(replay_manifest),
    )

    comparison = compare_outputs(
        original_data,
        replay_data,
    )

    comparison["historical_gold_run_id"] = arguments.gold_run_id
    comparison["replay_gold_run_id"] = replay_run_id
    comparison["integration_run_id"] = integration_run_id
    comparison["contamination"] = contamination

    print(json.dumps(comparison, indent=2))

    if not (
        comparison["same_record_count"]
        and comparison["predictions_identical"]
        and comparison["scores_close"]
    ):
        raise RuntimeError("Gold replay comparison failed")


if __name__ == "__main__":
    main()