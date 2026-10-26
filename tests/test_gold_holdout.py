# Role: Verify classification metrics, detect augmentation-family leakage,
#  and reject missing ground-truth labels.

import pandas as pd
import pytest

from pipeline.evaluation.evaluate_gold_holdout import (
    evaluate_holdout,
)


def sample():
    return pd.DataFrame({
        "dataset_split": [
            "train",
            "train",
            "test",
            "test",
            "test",
            "test",
        ],
        "augmentation_family_id": [
            "a",
            "b",
            "c",
            "d",
            "e",
            "f",
        ],
        "ground_truth_anomaly": [
            0,
            1,
            0,
            0,
            1,
            1,
        ],
        "anomaly_prediction": [
            1,
            -1,
            1,
            -1,
            -1,
            1,
        ],
    })


def test_confusion_matrix():
    result = evaluate_holdout(sample())

    assert (
        result["tp"],
        result["tn"],
        result["fp"],
        result["fn"],
    ) == (1, 1, 1, 1)

    assert result["accuracy"] == 0.5
    assert result["f1"] == 0.5


def test_leakage_rejected():
    frame = sample()

    frame.loc[
        2,
        "augmentation_family_id"
    ] = "a"

    with pytest.raises(
        ValueError,
        match="leakage",
    ):
        evaluate_holdout(frame)


def test_missing_labels_rejected():
    frame = sample().drop(
        columns=["ground_truth_anomaly"]
    )

    with pytest.raises(
        ValueError,
        match="Missing required",
    ):
        evaluate_holdout(frame)