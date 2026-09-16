import argparse
import json
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer


from pipeline.common.config import (
    KAFKA_SERVER,
    KAFKA_TOPIC,
)

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input-file",
        required=True,
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    input_path = Path(args.input_file)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    dataframe = pd.read_csv(input_path)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: (
            json.dumps(value).encode("utf-8")
        ),
    )

    print("\nQuality-test producer")
    print("---------------------")
    print(f"Kafka server: {KAFKA_SERVER}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"Records loaded: {len(dataframe)}")

    for _, row in dataframe.iterrows():

        record = row.to_dict()

        producer.send(
            KAFKA_TOPIC,
            value=record,
        )

    producer.flush()
    producer.close()

    print(
        f"\nSent {len(dataframe)} "
        "controlled test records."
    )


if __name__ == "__main__":
    main()