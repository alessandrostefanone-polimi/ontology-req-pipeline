import rdflib, networkx as nx
from pyvis.network import Network
from pathlib import Path
import re
from urllib.parse import quote


def _detect_format(text: str) -> str:
    """Choose rdflib parse format based on file contents (TTL vs RDF/XML)."""
    head = text.lstrip()
    if head.startswith("<?xml") or head.startswith("<rdf:RDF"):
        return "xml"
    return "turtle"


def _windows_path_to_file_uri(value: str) -> str:
    """Convert Windows path-like IRI values into valid file:// URIs."""
    norm = value.replace("\\", "/")
    if re.match(r"^[A-Za-z]:/", norm):
        return "file:///" + quote(norm, safe="/:#?&=@[]!$&'()*+,;")
    return value


def _normalize_invalid_xml_iris(xml_text: str) -> str:
    """Patch invalid RDF/XML attributes that contain raw Windows paths."""
    pattern = re.compile(r'="([A-Za-z]:\\[^"]*)"')

    def repl(match: re.Match) -> str:
        raw = match.group(1)
        return f'="{_windows_path_to_file_uri(raw)}"'

    return pattern.sub(repl, xml_text)


def _parse_graph(path: Path) -> rdflib.Graph:
    text = path.read_text(encoding="utf-8", errors="ignore")
    fmt = _detect_format(text)
    g = rdflib.Graph()
    if fmt == "xml":
        try:
            return g.parse(path.as_posix(), format="xml")
        except Exception:
            fixed_xml = _normalize_invalid_xml_iris(text)
            return rdflib.Graph().parse(data=fixed_xml, format="xml")
    return g.parse(path.as_posix(), format="turtle")


def kg_to_html(owl_path: str, output_html: str) -> None:
    owl_file = Path(owl_path)
    g = _parse_graph(owl_file)

    # Build a directed multigraph
    G = nx.MultiDiGraph()
    for s, p, o in g:
        # keep IRIs short for display
        s_label = s.split("/")[-1] if isinstance(s, rdflib.term.URIRef) else str(s)
        p_label = p.split("/")[-1] if isinstance(p, rdflib.term.URIRef) else str(p)
        if isinstance(o, rdflib.term.URIRef):
            o_label = o.split("/")[-1]
        else:
            o_label = f"\"{o}\""
        G.add_node(s_label)
        G.add_node(o_label)
        G.add_edge(s_label, o_label, label=p_label)

    net = Network(height="900px", width="100%", directed=True, bgcolor="#ffffff", notebook=True)
    net.from_nx(G)
    net.repulsion(node_distance=200, central_gravity=0.33,
                spring_length=110, spring_strength=0.10,
                damping=0.95)
    net.show(output_html)  # writes file next to the notebook

if __name__ == "__main__":
    base_dir = Path(__file__).parent
    kg_to_html(
        owl_path=(base_dir / "final_kg_7261.ttl").as_posix(),
        output_html=(base_dir / "final_KG.html").as_posix(),
    )
    # kg_to_html(
    #     owl_path=(base_dir / "initial_kg.owl").as_posix(),
    #     output_html=(base_dir / "initial_KG.html").as_posix(),
    # )
    # kg_to_html(
    #     owl_path=(base_dir / "final_kg_inferred_0.owl").as_posix(),
    #     output_html=(base_dir / "final_KG_inferred.html").as_posix(),
    # )
