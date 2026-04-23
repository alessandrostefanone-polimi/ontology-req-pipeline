from __future__ import annotations

from pathlib import Path
import textwrap

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import networkx as nx
from rdflib import Graph
from rdflib.namespace import OWL, RDF
from rdflib.term import Literal, Node, URIRef


TYPE_PREDICATE = str(RDF.type)
ONTOLOGY_CLASS = str(OWL.Ontology)


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def _local_name(iri: str) -> str:
    if "#" in iri:
        return iri.rsplit("#", 1)[1]
    if "/" in iri:
        return iri.rsplit("/", 1)[1]
    return iri


def _format_term(term: Node, graph: Graph, *, max_width: int, literal: bool = False) -> str:
    if isinstance(term, Literal):
        value = str(term)
        value = _truncate(value, max_width * 3)
        return textwrap.fill(f"\"{value}\"", width=max_width)

    if isinstance(term, URIRef):
        try:
            prefix, _, local = graph.namespace_manager.compute_qname(str(term))
            compact = f"{prefix}:{local}" if prefix else local
        except Exception:
            compact = _local_name(str(term))
        compact = _truncate(compact, max_width * 2)
        return textwrap.fill(compact, width=max_width)

    raw = _truncate(str(term), max_width * 2)
    return textwrap.fill(raw, width=max_width)


def rdflib_to_networkx(graph: Graph) -> nx.MultiDiGraph:
    rdf_graph = nx.MultiDiGraph()
    for subject, predicate, obj in graph:
        if predicate == RDF.type and obj == OWL.Ontology:
            continue
        rdf_graph.add_node(str(subject), is_literal=False)
        rdf_graph.add_node(str(obj), is_literal=isinstance(obj, Literal))
        rdf_graph.add_edge(
            str(subject),
            str(obj),
            predicate=str(predicate),
            is_type_edge=str(predicate) == TYPE_PREDICATE,
        )
    return rdf_graph


def _parse_rdf_file(path: Path) -> Graph:
    text = path.read_text(encoding="utf-8", errors="ignore").lstrip()
    rdf_format = "xml" if text.startswith("<?xml") or text.startswith("<rdf:RDF") else "turtle"
    return Graph().parse(path.as_posix(), format=rdf_format)


def _build_term_lookup(graph: Graph) -> dict[str, Node]:
    lookup: dict[str, Node] = {}
    for subject, predicate, obj in graph:
        if predicate == RDF.type and obj == OWL.Ontology:
            continue
        lookup[str(subject)] = subject
        lookup[str(obj)] = obj
    return lookup


def _rank_nodes(graph: nx.MultiDiGraph) -> dict[str, int]:
    simple = nx.DiGraph()
    simple.add_nodes_from(graph.nodes(data=True))
    simple.add_edges_from(graph.edges())

    remaining = set(simple.nodes())
    ranks: dict[str, int] = {}
    current_roots = sorted(
        [
            node
            for node, data in simple.nodes(data=True)
            if not data.get("is_literal") and simple.in_degree(node) == 0
        ]
    )

    while remaining:
        if not current_roots:
            current_roots = [
                min(
                    remaining,
                    key=lambda node: (
                        graph.nodes[node].get("is_literal", False),
                        simple.in_degree(node),
                        -simple.out_degree(node),
                        str(node),
                    ),
                )
            ]

        queue = list(current_roots)
        while queue:
            node = queue.pop(0)
            if node not in remaining:
                continue
            remaining.remove(node)

            predecessors = [pred for pred in simple.predecessors(node) if pred in ranks]
            rank = (max(ranks[pred] for pred in predecessors) + 1) if predecessors else 0
            ranks[node] = rank

            successors = sorted(
                simple.successors(node),
                key=lambda succ: (
                    graph.nodes[succ].get("is_literal", False),
                    str(succ),
                ),
            )
            for successor in successors:
                if successor in remaining and successor not in queue:
                    queue.append(successor)

        current_roots = sorted(
            [
                node
                for node in remaining
                if not graph.nodes[node].get("is_literal") and simple.in_degree(node) == 0
            ]
        )

    return ranks


def _layered_layout(graph: nx.MultiDiGraph) -> dict[str, tuple[float, float]]:
    ranks = _rank_nodes(graph)
    rank_to_nodes: dict[int, list[str]] = {}
    for node, rank in ranks.items():
        rank_to_nodes.setdefault(rank, []).append(node)

    positions: dict[str, tuple[float, float]] = {}
    x_gap = 4.5
    y_gap = 2.4

    for rank, nodes in sorted(rank_to_nodes.items()):
        ordered = sorted(
            nodes,
            key=lambda node: (
                graph.nodes[node].get("is_literal", False),
                -graph.out_degree(node),
                str(node),
            ),
        )
        count = len(ordered)
        for index, node in enumerate(ordered):
            y = ((count - 1) / 2.0 - index) * y_gap
            positions[node] = (rank * x_gap, y)

    return positions


def render_rdf_file_to_png(
    rdf_path: str | Path,
    output_png: str | Path,
    *,
    title: str | None = None,
    layout_seed: int = 7,
) -> Path:
    rdf_file = Path(rdf_path)
    png_file = Path(output_png)
    graph = _parse_rdf_file(rdf_file)
    nx_graph = rdflib_to_networkx(graph)
    term_lookup = _build_term_lookup(graph)

    if not nx_graph.nodes:
        raise ValueError(f"RDF graph is empty: {rdf_file}")

    position = _layered_layout(nx_graph)

    plt.figure(figsize=(22, 14))
    literal_nodes = [node for node, data in nx_graph.nodes(data=True) if data.get("is_literal")]
    iri_nodes = [node for node, data in nx_graph.nodes(data=True) if not data.get("is_literal")]
    type_edges = [(u, v) for u, v, data in nx_graph.edges(data=True) if data.get("is_type_edge")]
    relation_edges = [(u, v) for u, v, data in nx_graph.edges(data=True) if not data.get("is_type_edge")]

    nx.draw_networkx_nodes(
        nx_graph,
        position,
        nodelist=iri_nodes,
        node_size=[2600 + 28 * len(str(node)) for node in iri_nodes],
        node_color="#dbeafe",
        edgecolors="#1d4ed8",
        linewidths=1.4,
        node_shape="o",
    )
    nx.draw_networkx_nodes(
        nx_graph,
        position,
        nodelist=literal_nodes,
        node_size=[2200 + 18 * len(str(node)) for node in literal_nodes],
        node_color="#fef3c7",
        edgecolors="#b45309",
        linewidths=1.2,
        node_shape="s",
    )

    nx.draw_networkx_labels(
        nx_graph,
        position,
        labels={
            node: _format_term(
                term_lookup.get(node, node),
                graph,
                max_width=22 if node in literal_nodes else 16,
                literal=node in literal_nodes,
            )
            for node in nx_graph.nodes
        },
        font_size=10,
        font_weight="bold",
    )
    nx.draw_networkx_edges(
        nx_graph,
        position,
        edgelist=relation_edges,
        arrows=True,
        arrowstyle="-|>",
        arrowsize=18,
        width=1.6,
        edge_color="#6b7280",
        connectionstyle="arc3,rad=0.03",
        min_source_margin=12,
        min_target_margin=12,
    )
    nx.draw_networkx_edges(
        nx_graph,
        position,
        edgelist=type_edges,
        arrows=True,
        arrowstyle="-|>",
        arrowsize=15,
        width=1.0,
        edge_color="#94a3b8",
        style="dashed",
        connectionstyle="arc3,rad=0.0",
        min_source_margin=12,
        min_target_margin=12,
    )

    edge_labels: dict[tuple[str, str], str] = {}
    for source, target, data in nx_graph.edges(data=True):
        key = (source, target)
        if key not in edge_labels:
            predicate = data["predicate"]
            if predicate == TYPE_PREDICATE:
                edge_labels[key] = "a"
            else:
                edge_labels[key] = _format_term(URIRef(predicate), graph, max_width=14)

    nx.draw_networkx_edge_labels(
        nx_graph,
        position,
        edge_labels=edge_labels,
        font_size=12,
        rotate=False,
        bbox={"facecolor": "white", "edgecolor": "#e5e7eb", "alpha": 0.95, "pad": 0.3},
    )

    if title:
        plt.title(title, fontsize=24, fontweight="bold")
    plt.axis("off")
    plt.tight_layout()

    png_file.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(png_file, dpi=220, bbox_inches="tight", facecolor="white")
    plt.close()
    return png_file.resolve()
