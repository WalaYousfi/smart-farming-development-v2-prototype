"""Evaluate held-out Isolation Forest predictions against independent labels.

Python 3.7 compatible. Reads completed Gold Parquet from the configured MinIO
bucket and writes an evaluation JSON object to the same bucket.
"""

import argparse
import json
from io import BytesIO

import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    precision_recall_fscore_support,
)

from pipeline.common.config import GOLD_PREFIX, MINIO_BUCKET
from pipeline.common.minio_client import create_minio_client


def evaluate_holdout(frame):
    required = (
        "dataset_split",
        "ground_truth_anomaly",
        "anomaly_prediction",
        "augmentation_family_id",
    )

    missing = [
        name
        for name in required
        if name not in frame.columns
    ]

    if missing:
        raise ValueError(
            "Missing required columns: {}".format(missing)
        )

    if frame[list(required)].isnull().any().any():
        raise ValueError(
            "Missing split, label, prediction, or family metadata"
        )

    if not frame["dataset_split"].isin(["train", "test"]).all():
        raise ValueError("Invalid dataset_split values")

    if not frame["ground_truth_anomaly"].isin([0, 1]).all():
        raise ValueError(
            "Labels must be 0=normal or 1=anomaly"
        )

    if not frame["anomaly_prediction"].isin([-1, 1]).all():
        raise ValueError(
            "Predictions must be -1=anomaly or 1=normal"
        )

    train = frame.loc[
        frame["dataset_split"] == "train"
    ]

    test = frame.loc[
        frame["dataset_split"] == "test"
    ]

    if train.empty or test.empty:
        raise ValueError(
            "Both train and test partitions must be present"
        )

    train_families = set(
        train["augmentation_family_id"].astype(str)
    )

    test_families = set(
        test["augmentation_family_id"].astype(str)
    )

    if train_families.intersection(test_families):
        raise ValueError(
            "Augmentation-family leakage across train/test"
        )

    # Evaluate only the held-out test partition.
    y_true = test[
        "ground_truth_anomaly"
    ].astype(int).to_numpy()

    # Isolation Forest:
    # -1 = anomaly, 1 = normal.
    # Evaluation:
    #  1 = anomaly, 0 = normal.
    y_pred = (
        test["anomaly_prediction"]
        .astype(int)
        .to_numpy() == -1
    ).astype(int)

    tn, fp, fn, tp = confusion_matrix(
        y_true,
        y_pred,
        labels=[0, 1],
    ).ravel()

    precision, recall, f1, _ = (
        precision_recall_fscore_support(
            y_true,
            y_pred,
            labels=[1],
            zero_division=0,
        )
    )

    specificity = (
        float(tn) / (tn + fp)
        if tn + fp
        else None
    )

    return {
        "evaluation_scope": "test_only",
        "train_records": int(len(train)),
        "test_records": int(len(test)),
        "test_true_anomalies": int(y_true.sum()),
        "test_predicted_anomalies": int(y_pred.sum()),
        "tp": int(tp),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
        "accuracy": float(
            accuracy_score(y_true, y_pred)
        ),
        "precision": float(precision[0]),
        "recall": float(recall[0]),
        "f1": float(f1[0]),
        "specificity": specificity,
        "positive_class": "anomaly",
        "ground_truth_source": "upstream_independent_labels",
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--gold-run-id",
        required=True,
    )

    args = parser.parse_args()

    client = create_minio_client()

    scored_key = (
        "{}/analytical/integrated_field_weather_scores/"
        "run_id={}/scored_integrated_observations.parquet"
    ).format(
        GOLD_PREFIX,
        args.gold_run_id,
    )

    response = client.get_object(
        MINIO_BUCKET,
        scored_key,
    )

    try:
        scored = pd.read_parquet(
            BytesIO(response.read())
        )
    finally:
        response.close()
        response.release_conn()

    result = evaluate_holdout(scored)

    result["gold_run_id"] = args.gold_run_id

    payload = json.dumps(
        result,
        indent=2,
        sort_keys=True,
    ).encode("utf-8")

    key = (
        "{}/evaluation/holdout_metrics/"
        "run_id={}/metrics.json"
    ).format(
        GOLD_PREFIX,
        args.gold_run_id,
    )

    client.put_object(
        MINIO_BUCKET,
        key,
        BytesIO(payload),
        len(payload),
        content_type="application/json",
    )

    print(
        json.dumps(
            result,
            indent=2,
            sort_keys=True,
        )
    )

    print(
        "Stored in bucket {} at {}".format(
            MINIO_BUCKET,
            key,
        )
    )


if __name__ == "__main__":
    main()