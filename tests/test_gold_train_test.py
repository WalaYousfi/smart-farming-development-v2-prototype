	# role: Verify that training and testing remain separate
import numpy as np
import pandas as pd

from pipeline.gold.integrated_anomaly_detection import (
    create_train_test_split,
)


def test_split_has_no_overlap():
    dataframe = pd.DataFrame({
        "augmentation_family_id": [
            "family_{}".format(i // 10)
            for i in range(100)
        ]
    })

    train_indices, test_indices = (
        create_train_test_split(dataframe)
    )

    assert len(
        set(train_indices).intersection(
            set(test_indices)
        )
    ) == 0

    assert (
        len(train_indices) + len(test_indices)
        == len(dataframe)
    )


def test_families_do_not_cross_splits():
    dataframe = pd.DataFrame({
        "augmentation_family_id": [
            "family_{}".format(i // 10)
            for i in range(100)
        ]
    })

    train_indices, test_indices = (
        create_train_test_split(dataframe)
    )

    train_families = set(
        dataframe.iloc[train_indices][
            "augmentation_family_id"
        ]
    )

    test_families = set(
        dataframe.iloc[test_indices][
            "augmentation_family_id"
        ]
    )

    assert train_families.isdisjoint(
        test_families
    )


def test_split_is_reproducible():
    dataframe = pd.DataFrame({
        "augmentation_family_id": [
            "family_{}".format(i // 10)
            for i in range(100)
        ]
    })

    first_train, first_test = (
        create_train_test_split(dataframe)
    )

    second_train, second_test = (
        create_train_test_split(dataframe)
    )

    assert np.array_equal(
        first_train,
        second_train,
    )

    assert np.array_equal(
        first_test,
        second_test,
    )