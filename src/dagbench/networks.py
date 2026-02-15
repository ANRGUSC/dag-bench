"""Network topology presets for DAGs without network descriptions.

All networks are fully connected (required by SAGA's HEFT scheduler).
Network.create() auto-adds self-loops and missing edges, but fills them
with speed=0 which causes division-by-zero. We explicitly provide all
pairs with appropriate bandwidths.
"""
from __future__ import annotations

from saga import Network


def _all_pairs_edges(node_names: list[str], get_bw) -> list[tuple[str, str, float]]:
    """Generate edges for all unique pairs of nodes."""
    edges = []
    for i, n1 in enumerate(node_names):
        for j in range(i + 1, len(node_names)):
            n2 = node_names[j]
            edges.append((n1, n2, get_bw(n1, n2)))
    return edges


def fog_network(
    num_edge: int = 10,
    num_fog: int = 3,
    num_cloud: int = 2,
    edge_speed: float = 1.0,
    fog_speed: float = 8.0,
    cloud_speed: float = 50.0,
    edge_fog_bw: float = 7500.0,
    fog_cloud_bw: float = 12500.0,
    fog_fog_bw: float = 12500.0,
    cloud_cloud_bw: float = 125_000_000.0,
    edge_edge_bw: float = 500.0,
    edge_cloud_bw: float = 1000.0,
) -> Network:
    """3-tier IoT/fog network topology (fully connected)."""
    nodes = []
    for i in range(num_edge):
        nodes.append((f"E{i}", edge_speed))
    for i in range(num_fog):
        nodes.append((f"F{i}", fog_speed))
    for i in range(num_cloud):
        nodes.append((f"C{i}", cloud_speed))

    node_names = [n[0] for n in nodes]

    def get_bw(n1: str, n2: str) -> float:
        t1, t2 = n1[0], n2[0]
        pair = tuple(sorted([t1, t2]))
        bw_map = {
            ("E", "E"): edge_edge_bw,
            ("E", "F"): edge_fog_bw,
            ("E", "C"): edge_cloud_bw,
            ("F", "F"): fog_fog_bw,
            ("C", "F"): fog_cloud_bw,
            ("C", "C"): cloud_cloud_bw,
        }
        return bw_map.get(pair, edge_edge_bw)

    edges = _all_pairs_edges(node_names, get_bw)
    return Network.create(nodes=nodes, edges=edges)


def homogeneous_network(
    num_nodes: int = 4,
    speed: float = 1.0,
    bandwidth: float = 1.0,
) -> Network:
    """Fully-connected homogeneous network."""
    nodes = [(f"N{i}", speed) for i in range(num_nodes)]
    edges = []
    for i in range(num_nodes):
        for j in range(i + 1, num_nodes):
            edges.append((f"N{i}", f"N{j}", bandwidth))
    return Network.create(nodes=nodes, edges=edges)


def star_network(
    num_edge: int = 4,
    hub_speed: float = 10.0,
    edge_speed: float = 1.0,
    bandwidth: float = 1000.0,
    edge_edge_bw: float = 100.0,
) -> Network:
    """Star topology with a central hub (fully connected)."""
    nodes = [("Hub", hub_speed)]
    for i in range(num_edge):
        nodes.append((f"E{i}", edge_speed))

    edges = []
    # Hub to each edge
    for i in range(num_edge):
        edges.append(("Hub", f"E{i}", bandwidth))
    # Edge to edge (lower bandwidth, goes through hub)
    for i in range(num_edge):
        for j in range(i + 1, num_edge):
            edges.append((f"E{i}", f"E{j}", edge_edge_bw))

    return Network.create(nodes=nodes, edges=edges)


def mec_network(
    num_ue: int = 4,
    num_mec: int = 2,
    ue_speed: float = 1.0,
    mec_speed: float = 10.0,
    cloud_speed: float = 50.0,
    ue_mec_bw: float = 5000.0,
    ue_cloud_bw: float = 500.0,
    mec_cloud_bw: float = 15000.0,
    mec_mec_bw: float = 10000.0,
    ue_ue_bw: float = 200.0,
) -> Network:
    """MEC network: UE devices + MEC servers + cloud (fully connected)."""
    nodes = []
    for i in range(num_ue):
        nodes.append((f"UE{i}", ue_speed))
    for i in range(num_mec):
        nodes.append((f"MEC{i}", mec_speed))
    nodes.append(("Cloud", cloud_speed))

    node_names = [n[0] for n in nodes]

    def get_bw(n1: str, n2: str) -> float:
        def tier(n):
            if n.startswith("UE"):
                return "U"
            if n.startswith("MEC"):
                return "M"
            return "C"
        t1, t2 = tier(n1), tier(n2)
        pair = tuple(sorted([t1, t2]))
        bw_map = {
            ("U", "U"): ue_ue_bw,
            ("M", "U"): ue_mec_bw,
            ("C", "U"): ue_cloud_bw,
            ("M", "M"): mec_mec_bw,
            ("C", "M"): mec_cloud_bw,
            ("C", "C"): 125_000_000.0,
        }
        return bw_map.get(pair, ue_ue_bw)

    edges = _all_pairs_edges(node_names, get_bw)
    return Network.create(nodes=nodes, edges=edges)


def manet_network(
    num_nodes: int = 6,
    node_speed: float = 1.0,
    command_speed: float = 5.0,
    node_bw: float = 1000.0,
    command_bw: float = 5000.0,
    node_node_bw: float = 500.0,
) -> Network:
    """MANET/tactical mesh: command node + mobile peers (fully connected)."""
    nodes = [("CMD", command_speed)]
    for i in range(num_nodes):
        nodes.append((f"N{i}", node_speed))

    node_names = [n[0] for n in nodes]

    def get_bw(n1: str, n2: str) -> float:
        if n1 == "CMD" or n2 == "CMD":
            return command_bw
        return node_node_bw

    edges = _all_pairs_edges(node_names, get_bw)
    return Network.create(nodes=nodes, edges=edges)
