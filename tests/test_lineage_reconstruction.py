# Role: Verify that multiple branches can share a parent without duplicating nodes 
# and that missing dependencies are reported.

from pipeline.evaluation.evaluate_lineage_traceability import (
    reconstruct_graph,
)


def make_lineage(run_id, parents):
    return {
        "run_id": run_id,
        "job_name": "test_transformation",
        "input_zone": "silver",
        "output_zone": "gold",
        "parent_run_ids": parents,
        "input_objects": [],
        "output_objects": [],
    }


def test_shared_dependency():
    lineage_index = {
        "gold": make_lineage(
            "gold", ["integration"]
        ),
        "integration": make_lineage(
            "integration", ["field", "weather"]
        ),
        "field": make_lineage(
            "field", ["shared"]
        ),
        "weather": make_lineage(
            "weather", ["shared"]
        ),
        "shared": make_lineage(
            "shared", []
        ),
    }

    nodes, edges = reconstruct_graph(
        "gold",
        lineage_index,
    )

    assert len(nodes) == 5
    assert len(edges) == 5

    assert "shared" in nodes


def test_missing_dependency():
    lineage_index = {
        "gold": make_lineage(
            "gold", ["missing-run"]
        ),
    }

    nodes, edges = reconstruct_graph(
        "gold",
        lineage_index,
    )

    assert nodes["missing-run"]["node_type"] == (
        "unresolved"
    )

    assert len(edges) == 1


def test_missing_start_run():
    nodes, edges = reconstruct_graph(
        "unknown-gold",
        {},
    )

    assert nodes["unknown-gold"]["node_type"] == (
        "unresolved"
    )

    assert len(edges) == 0