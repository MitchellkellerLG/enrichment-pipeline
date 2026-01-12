"""
DAG Definition and Dependency Resolution for Enrichment Pipeline

This module defines the directed acyclic graph (DAG) of enrichment nodes
and provides functions for determining node readiness and execution order.

Nodes:
    mx_check -> icp_check -> [careers, news] -> webhook_push

Gates:
    - mx_pass: Set by mx_check, required for icp_check
    - icp_pass: Set by icp_check, required for careers and news
"""

from dataclasses import dataclass, field
from typing import Optional, Protocol, Any


# =============================================================================
# Type Definitions
# =============================================================================

class StateStore(Protocol):
    """Protocol for state store interface used by DAG resolution functions."""

    def get_node_status(self, company_key: str, node_name: str, run_id: str) -> Optional[str]:
        """Get status of a node: 'pending', 'running', 'completed', 'skipped', or None."""
        ...

    def get_gate(self, company_key: str, gate_name: str, run_id: str) -> Optional[bool]:
        """Get gate value: True, False, or None (not yet evaluated)."""
        ...

    def set_node_status(self, company_key: str, node_name: str, run_id: str, status: str) -> None:
        """Set status of a node."""
        ...


# =============================================================================
# Node Definition
# =============================================================================

@dataclass
class Node:
    """
    A node in the enrichment pipeline DAG.

    Attributes:
        name: Unique identifier for this node
        depends_on: List of node names that must complete before this node can run
        gate_requires: Optional gate name that must be True for this node to run.
                      If the gate is False, the node will be skipped.
                      If the gate is None (not yet evaluated), the node is not ready.
        provider: Provider name for semaphore/rate limiting (e.g., "openrouter", "spider")
    """
    name: str
    depends_on: list[str] = field(default_factory=list)
    gate_requires: Optional[str] = None
    provider: str = ""


# =============================================================================
# Node Definitions
# =============================================================================

NODES: dict[str, Node] = {
    "mx_check": Node(
        name="mx_check",
        depends_on=[],
        gate_requires=None,
        provider="mx"
    ),
    "icp_check": Node(
        name="icp_check",
        depends_on=["mx_check"],
        gate_requires="mx_pass",
        provider="openrouter"
    ),
    "careers": Node(
        name="careers",
        depends_on=["icp_check"],
        gate_requires="icp_pass",
        provider="spider"
    ),
    "news": Node(
        name="news",
        depends_on=["icp_check"],
        gate_requires="icp_pass",
        provider="serper"
    ),
    "webhook_push": Node(
        name="webhook_push",
        depends_on=["careers", "news"],
        gate_requires=None,
        provider="webhook"
    ),
}


# =============================================================================
# DAG Resolution Functions
# =============================================================================

def get_ready_nodes(
    company_key: str,
    state_store: StateStore,
    run_id: str
) -> list[str]:
    """
    Get list of node names that are ready to run for a given company.

    A node is ready to run if:
    1. Its status is not 'completed', 'skipped', or 'running'
    2. All dependencies have status='completed'
    3. If gate_requires is set:
       - Gate is True: node is ready
       - Gate is False: node should be marked as skipped (done here)
       - Gate is None: node is not ready (gate not yet evaluated)

    Args:
        company_key: Unique identifier for the company being processed
        state_store: State store instance for querying/updating node state
        run_id: Current pipeline run identifier

    Returns:
        List of node names that are ready to execute
    """
    ready_nodes: list[str] = []

    for node_name, node in NODES.items():
        # Check current status - skip if already done or in progress
        current_status = state_store.get_node_status(company_key, node_name, run_id)
        if current_status in ("completed", "skipped", "running"):
            continue

        # Check all dependencies are completed
        dependencies_met = True
        for dep_name in node.depends_on:
            dep_status = state_store.get_node_status(company_key, dep_name, run_id)
            if dep_status != "completed":
                dependencies_met = False
                break

        if not dependencies_met:
            continue

        # Check gate requirement
        if node.gate_requires is not None:
            gate_value = state_store.get_gate(company_key, node.gate_requires, run_id)

            if gate_value is None:
                # Gate not yet evaluated - node not ready
                continue
            elif gate_value is False:
                # Gate failed - mark node as skipped
                state_store.set_node_status(company_key, node_name, run_id, "skipped")
                continue
            # gate_value is True - node is ready to proceed

        # All checks passed - node is ready
        ready_nodes.append(node_name)

    return ready_nodes


def is_company_complete(
    company_key: str,
    state_store: StateStore,
    run_id: str
) -> bool:
    """
    Check if all nodes for a company are either completed or skipped.

    Args:
        company_key: Unique identifier for the company being processed
        state_store: State store instance for querying node state
        run_id: Current pipeline run identifier

    Returns:
        True if all nodes are in a terminal state (completed or skipped),
        False otherwise
    """
    for node_name in NODES:
        status = state_store.get_node_status(company_key, node_name, run_id)
        if status not in ("completed", "skipped"):
            return False
    return True


def get_node_order() -> list[str]:
    """
    Get topologically sorted list of node names.

    Returns nodes in an order where dependencies come before dependents.
    Uses Kahn's algorithm for topological sorting.

    Returns:
        List of node names in topological order

    Raises:
        ValueError: If the graph contains a cycle
    """
    # Build in-degree map and adjacency list
    in_degree: dict[str, int] = {name: 0 for name in NODES}
    dependents: dict[str, list[str]] = {name: [] for name in NODES}

    for node_name, node in NODES.items():
        in_degree[node_name] = len(node.depends_on)
        for dep_name in node.depends_on:
            dependents[dep_name].append(node_name)

    # Find all nodes with no dependencies
    queue: list[str] = [name for name, degree in in_degree.items() if degree == 0]
    result: list[str] = []

    while queue:
        # Pop from front for stable ordering
        current = queue.pop(0)
        result.append(current)

        # Reduce in-degree of dependents
        for dependent in dependents[current]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    # Check for cycles
    if len(result) != len(NODES):
        remaining = [name for name in NODES if name not in result]
        raise ValueError(f"DAG contains a cycle involving nodes: {remaining}")

    return result


def get_node(node_name: str) -> Optional[Node]:
    """
    Get a node by name.

    Args:
        node_name: Name of the node to retrieve

    Returns:
        Node instance if found, None otherwise
    """
    return NODES.get(node_name)


def get_all_nodes() -> dict[str, Node]:
    """
    Get all nodes in the DAG.

    Returns:
        Dictionary mapping node names to Node instances
    """
    return NODES.copy()


def get_provider_nodes(provider: str) -> list[str]:
    """
    Get all nodes that use a specific provider.

    Useful for rate limiting and semaphore management.

    Args:
        provider: Provider name (e.g., "openrouter", "spider")

    Returns:
        List of node names that use the specified provider
    """
    return [name for name, node in NODES.items() if node.provider == provider]


# =============================================================================
# Validation
# =============================================================================

def validate_dag() -> bool:
    """
    Validate the DAG structure.

    Checks:
    1. All dependencies reference existing nodes
    2. No cycles exist
    3. All providers are non-empty

    Returns:
        True if DAG is valid

    Raises:
        ValueError: If validation fails with details about the issue
    """
    # Check all dependencies exist
    for node_name, node in NODES.items():
        for dep_name in node.depends_on:
            if dep_name not in NODES:
                raise ValueError(
                    f"Node '{node_name}' depends on unknown node '{dep_name}'"
                )

    # Check for cycles (will raise if cycle found)
    get_node_order()

    # Check providers are set
    for node_name, node in NODES.items():
        if not node.provider:
            raise ValueError(f"Node '{node_name}' has no provider set")

    return True
