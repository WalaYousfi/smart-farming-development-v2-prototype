import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from pipeline.common.config import (
    LINEAGE_PREFIX,
    MINIO_BUCKET,
    PROJECT_ROOT,
)
from pipeline.common.minio_client import (
    create_minio_client,
)


EXPECTED_NODE_COUNT = 6
EXPECTED_EDGE_COUNT = 5
EXPECTED_BRONZE_ROOTS = 2
EXPECTED_TRANSFORMATION_LINEAGES = 4


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Reconstruct and evaluate end-to-end lineage "
            "starting from one integrated Gold run."
        )
    )

    parser.add_argument(
        "--gold-run-id",
        required=True,
        help="Integrated Gold run from which ancestry is reconstructed.",
    )

    parser.add_argument(
        "--experiment-name",
        default="end-to-end-lineage-evaluation",
    )

    return parser.parse_args()


def read_json_object(
    minio_client: Any,
    object_name: str,
) -> Dict[str, Any]:
    response = minio_client.get_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
    )

    try:
        return json.loads(
            response.read().decode("utf-8")
        )

    finally:
        response.close()
        response.release_conn()


def build_lineage_index(
    minio_client: Any,
) -> Dict[str, Dict[str, Any]]:
    """
    Read every lineage record and index it by run_id.
    """

    objects = minio_client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix=f"{LINEAGE_PREFIX}/",
        recursive=True,
    )

    lineage_index = {}

    for obj in objects:
        if not obj.object_name.endswith(
            "lineage.json"
        ):
            continue

        lineage = read_json_object(
            minio_client=minio_client,
            object_name=obj.object_name,
        )

        run_id = lineage.get("run_id")

        if not run_id:
            continue

        lineage["_object_name"] = (
            obj.object_name
        )

        lineage_index[run_id] = lineage

    return lineage_index


def discover_source_information(
    lineage: Dict[str, Any],
    parent_run_id: str,
) -> Dict[str, Any]:
    """
    Infer information about a Bronze root from the
    input objects declared by its Silver child.
    """

    matching_objects = []

    for object_name in lineage.get(
        "input_objects",
        [],
    ):
        if (
            f"run_id={parent_run_id}/"
            in object_name
        ):
            matching_objects.append(
                object_name
            )

    source_system = None

    for object_name in matching_objects:
        parts = object_name.split("/")

        for part in parts:
            if part.startswith("source="):
                source_system = part.split(
                    "=",
                    1,
                )[1]

                break

        if source_system:
            break

    return {
        "run_id": parent_run_id,
        "node_type": "bronze_root",
        "job_name": None,
        "source_system": source_system,
        "input_objects_from_child": (
            matching_objects
        ),
    }


def reconstruct_graph(
    start_run_id: str,
    lineage_index: Dict[str, Dict[str, Any]],
) -> Tuple[
    Dict[str, Dict[str, Any]],
    List[Dict[str, str]],
]:
    """
    Recursively reconstruct all ancestors from
    the requested starting run.
    """

    nodes = {}
    edges = []

    visited: Set[str] = set()

    def visit(run_id: str) -> None:
        if run_id in visited:
            return

        visited.add(run_id)

        lineage = lineage_index.get(
            run_id
        )

        if lineage is None:
            nodes[run_id] = {
                "run_id": run_id,
                "node_type": "bronze_root",
                "job_name": None,
            }

            return

        job_name = lineage.get(
            "job_name"
        )

        nodes[run_id] = {
            "run_id": run_id,
            "node_type": "transformation",
            "job_name": job_name,
            "input_zone": lineage.get(
                "input_zone"
            ),
            "output_zone": lineage.get(
                "output_zone"
            ),
            "lineage_object": lineage.get(
                "_object_name"
            ),
        }

        parent_run_ids = lineage.get(
            "parent_run_ids",
            [],
        )

        for parent_run_id in parent_run_ids:
            edges.append(
                {
                    "parent_run_id": (
                        parent_run_id
                    ),
                    "child_run_id": run_id,
                }
            )

            if (
                parent_run_id
                not in lineage_index
            ):
                source_info = (
                    discover_source_information(
                        lineage=lineage,
                        parent_run_id=(
                            parent_run_id
                        ),
                    )
                )

                nodes[parent_run_id] = (
                    source_info
                )

            else:
                visit(parent_run_id)

    visit(start_run_id)

    return nodes, edges


def calculate_depth(
    run_id: str,
    lineage_index: Dict[str, Dict[str, Any]],
) -> int:
    """
    Calculate maximum ancestry depth from the
    requested run to a Bronze root.

    Gold → Integration → Silver → Bronze = 3 edges.
    """

    lineage = lineage_index.get(
        run_id
    )

    if lineage is None:
        return 0

    parent_run_ids = lineage.get(
        "parent_run_ids",
        [],
    )

    if not parent_run_ids:
        return 0

    return 1 + max(
        calculate_depth(
            parent_run_id,
            lineage_index,
        )
        for parent_run_id in parent_run_ids
    )


def calculate_metrics(
    nodes: Dict[str, Dict[str, Any]],
    edges: List[Dict[str, str]],
    lineage_index: Dict[str, Dict[str, Any]],
    gold_run_id: str,
) -> Dict[str, Any]:
    bronze_roots = [
        node
        for node in nodes.values()
        if node.get("node_type")
        == "bronze_root"
    ]

    transformation_nodes = [
        node
        for node in nodes.values()
        if node.get("node_type")
        == "transformation"
    ]

    source_systems = sorted(
        {
            node.get("source_system")
            for node in bronze_roots
            if node.get("source_system")
        }
    )

    integration_nodes = [
        node
        for node in transformation_nodes
        if node.get("job_name")
        == "silver_field_weather_integration"
    ]

    multi_parent_preserved = False

    if len(integration_nodes) == 1:
        integration_run_id = (
            integration_nodes[0][
                "run_id"
            ]
        )

        integration_lineage = (
            lineage_index.get(
                integration_run_id,
                {},
            )
        )

        multi_parent_preserved = (
            len(
                integration_lineage.get(
                    "parent_run_ids",
                    [],
                )
            )
            == 2
        )

    node_completeness = (
        len(nodes)
        / EXPECTED_NODE_COUNT
    )

    edge_completeness = (
        len(edges)
        / EXPECTED_EDGE_COUNT
    )

    lineage_record_completeness = (
        len(transformation_nodes)
        / EXPECTED_TRANSFORMATION_LINEAGES
    )

    bronze_root_completeness = (
        len(bronze_roots)
        / EXPECTED_BRONZE_ROOTS
    )

    complete = all(
        [
            len(nodes)
            == EXPECTED_NODE_COUNT,
            len(edges)
            == EXPECTED_EDGE_COUNT,
            len(bronze_roots)
            == EXPECTED_BRONZE_ROOTS,
            len(transformation_nodes)
            == EXPECTED_TRANSFORMATION_LINEAGES,
            multi_parent_preserved,
            len(source_systems) == 2,
        ]
    )

    return {
        "run_nodes_discovered": len(
            nodes
        ),
        "expected_run_nodes": (
            EXPECTED_NODE_COUNT
        ),
        "transformation_edges_discovered": (
            len(edges)
        ),
        "expected_transformation_edges": (
            EXPECTED_EDGE_COUNT
        ),
        "transformation_lineage_records": (
            len(transformation_nodes)
        ),
        "expected_transformation_lineages": (
            EXPECTED_TRANSFORMATION_LINEAGES
        ),
        "bronze_roots_discovered": (
            len(bronze_roots)
        ),
        "expected_bronze_roots": (
            EXPECTED_BRONZE_ROOTS
        ),
        "source_systems_recovered": (
            len(source_systems)
        ),
        "source_systems": source_systems,
        "maximum_lineage_depth": (
            calculate_depth(
                gold_run_id,
                lineage_index,
            )
        ),
        "node_completeness": round(
            node_completeness,
            4,
        ),
        "edge_completeness": round(
            edge_completeness,
            4,
        ),
        "lineage_record_completeness": round(
            lineage_record_completeness,
            4,
        ),
        "bronze_root_completeness": round(
            bronze_root_completeness,
            4,
        ),
        "multi_parent_integration_preserved": (
            multi_parent_preserved
        ),
        "end_to_end_source_ancestry_preserved": (
            complete
        ),
        "overall_traceability_status": (
            "complete"
            if complete
            else "incomplete"
        ),
    }


def build_ascii_tree(
    nodes: Dict[str, Dict[str, Any]],
) -> List[str]:
    """
    Produce a human-readable representation for
    the known prototype job structure.
    """

    by_job = {}

    roots = []

    for node in nodes.values():
        job_name = node.get(
            "job_name"
        )

        if job_name:
            by_job[job_name] = node
        else:
            roots.append(node)

    lines = []

    gold = by_job.get(
        "gold_integrated_field_anomaly_detection"
    )

    integration = by_job.get(
        "silver_field_weather_integration"
    )

    field_silver = by_job.get(
        "silver_field_observations"
    )

    weather_silver = by_job.get(
        "silver_weather_observations"
    )

    if gold:
        lines.append(
            f"Gold: {gold['run_id']}"
        )

    if integration:
        lines.append(
            f"└── Integrated Silver: "
            f"{integration['run_id']}"
        )

    if field_silver:
        lines.append(
            f"    ├── Field Silver: "
            f"{field_silver['run_id']}"
        )

        field_root = next(
            (
                root
                for root in roots
                if root.get(
                    "source_system"
                )
                == (
                    "smart_farming_crop_yield_csv"
                )
            ),
            None,
        )

        if field_root:
            lines.append(
                f"    │   └── Field Bronze: "
                f"{field_root['run_id']}"
            )

    if weather_silver:
        lines.append(
            f"    └── Weather Silver: "
            f"{weather_silver['run_id']}"
        )

        weather_root = next(
            (
                root
                for root in roots
                if root.get(
                    "source_system"
                )
                == "farm_weather_station"
            ),
            None,
        )

        if weather_root:
            lines.append(
                f"        └── Weather Bronze: "
                f"{weather_root['run_id']}"
            )

    return lines


def save_report(
    report: Dict[str, Any],
    experiment_name: str,
) -> Path:
    output_directory = (
        PROJECT_ROOT
        / "experiments"
        / "paper-evaluation"
        / "traceability"
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_directory
        / f"{experiment_name}.json"
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            report,
            file,
            indent=2,
            ensure_ascii=False,
        )

    return output_path


def main() -> None:
    args = parse_arguments()

    minio_client = (
        create_minio_client()
    )

    lineage_index = (
        build_lineage_index(
            minio_client
        )
    )

    if (
        args.gold_run_id
        not in lineage_index
    ):
        raise RuntimeError(
            "No lineage record was found for "
            f"Gold run: {args.gold_run_id}"
        )

    nodes, edges = reconstruct_graph(
        start_run_id=args.gold_run_id,
        lineage_index=lineage_index,
    )

    metrics = calculate_metrics(
        nodes=nodes,
        edges=edges,
        lineage_index=lineage_index,
        gold_run_id=args.gold_run_id,
    )

    tree = build_ascii_tree(
        nodes
    )

    report = {
        "experiment": (
            "end_to_end_lineage_traceability"
        ),
        "generated_at": utc_now(),
        "bucket": MINIO_BUCKET,
        "starting_gold_run_id": (
            args.gold_run_id
        ),
        "metrics": metrics,
        "nodes": list(
            nodes.values()
        ),
        "edges": edges,
        "tree": tree,
    }

    output_path = save_report(
        report=report,
        experiment_name=(
            args.experiment_name
        ),
    )

    print("\nTRACEABILITY EVALUATION")
    print("=======================")

    print(
        "\nStarting Gold run:"
    )
    print(args.gold_run_id)

    print("\nRecovered lineage graph")
    print("-----------------------")

    for line in tree:
        print(line)

    print("\nMetrics")
    print("-------")

    print(
        "Run nodes discovered: "
        f"{metrics['run_nodes_discovered']}/"
        f"{metrics['expected_run_nodes']}"
    )

    print(
        "Transformation edges discovered: "
        f"{metrics['transformation_edges_discovered']}/"
        f"{metrics['expected_transformation_edges']}"
    )

    print(
        "Transformation lineage records: "
        f"{metrics['transformation_lineage_records']}/"
        f"{metrics['expected_transformation_lineages']}"
    )

    print(
        "Bronze roots discovered: "
        f"{metrics['bronze_roots_discovered']}/"
        f"{metrics['expected_bronze_roots']}"
    )

    print(
        "Source systems recovered: "
        f"{metrics['source_systems_recovered']}"
    )

    print(
        "Maximum lineage depth: "
        f"{metrics['maximum_lineage_depth']}"
    )

    print(
        "Edge completeness: "
        f"{metrics['edge_completeness']:.4f}"
    )

    print(
        "Multi-parent integration preserved: "
        f"{metrics['multi_parent_integration_preserved']}"
    )

    print(
        "End-to-end source ancestry preserved: "
        f"{metrics['end_to_end_source_ancestry_preserved']}"
    )

    print(
        "Overall status: "
        f"{metrics['overall_traceability_status']}"
    )

    print("\nEvaluation report:")
    print(output_path)


if __name__ == "__main__":
    main()