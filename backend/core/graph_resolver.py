"""
core.graph_resolver
──────────────────────
Pure graph algorithm — no Spark, no DB, no HTTP. Given a ReactFlow-style
{nodes, edges} graph and a target_node_id, computes the minimal ordered
list of nodes that must execute to produce that node's output.

Because this module has zero I/O dependencies, it is fully unit-testable
with plain dicts/lists — no fixtures, no mocks required.
"""
from collections import defaultdict, deque


class GraphCycleError(Exception):
    pass


class NodeNotFoundError(Exception):
    pass


class DependencyResolver:
    def __init__(self, nodes: list[dict], edges: list[dict]):
        self.nodes_by_id = {n["id"]: n for n in nodes}
        self.edges = edges
        self.forward: dict[str, list[str]] = defaultdict(list)   # source -> [targets]
        self.backward: dict[str, list[str]] = defaultdict(list)  # target -> [sources]
        for e in edges:
            self.forward[e["source"]].append(e["target"])
            self.backward[e["target"]].append(e["source"])

    def ancestor_closure(self, target_node_id: str) -> set[str]:
        if target_node_id not in self.nodes_by_id:
            raise NodeNotFoundError(f"Node '{target_node_id}' not found in graph")
        visited: set[str] = set()
        queue = deque([target_node_id])
        while queue:
            cur = queue.popleft()
            if cur in visited:
                continue
            visited.add(cur)
            for parent in self.backward.get(cur, []):
                if parent not in visited:
                    queue.append(parent)
        return visited

    def topological_order(self, node_ids: set[str]) -> list[str]:
        indegree = {nid: 0 for nid in node_ids}
        local_forward = defaultdict(list)
        for nid in node_ids:
            for child in self.forward.get(nid, []):
                if child in node_ids:
                    local_forward[nid].append(child)
                    indegree[child] += 1

        queue = deque(sorted(n for n, d in indegree.items() if d == 0))
        order = []
        while queue:
            n = queue.popleft()
            order.append(n)
            for child in sorted(local_forward[n]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)

        if len(order) != len(node_ids):
            remaining = node_ids - set(order)
            raise GraphCycleError(f"Cycle detected involving nodes: {remaining}")
        return order

    def resolve_execution_plan(self, target_node_id: str) -> list[dict]:
        required_ids = self.ancestor_closure(target_node_id)
        ordered_ids = self.topological_order(required_ids)
        return [self.nodes_by_id[nid] for nid in ordered_ids]

    def fanout_node_ids(self, node_ids: set[str]) -> set[str]:
        """Nodes with more than one child WITHIN the resolved subgraph — good cache candidates."""
        return {
            nid for nid in node_ids
            if len([c for c in self.forward.get(nid, []) if c in node_ids]) > 1
        }