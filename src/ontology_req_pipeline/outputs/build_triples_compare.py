"""Build combined Vis-network graph comparing triples_IOF and triples_IOF_KG.

- Common nodes/edges = blue
- Inferred-only (triples_IOF) = red
- KG-only (triples_IOF_KG) = gray
- Includes toggle button in the HTML to hide/show inferred-only items.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Tuple
from ontology_req_pipeline.outputs.kg_to_html import kg_to_html

ROOT = Path(__file__).resolve().parent
INPUT_INFERRED = ROOT / "final_KG_inferred.html"
INPUT_KG = ROOT / "final_KG.html"
OUTPUT = ROOT / "./KG_compare.html"

COLOR_COMMON = "#1f77b4"   # blue
COLOR_INFERRED = "#d62728" # red
COLOR_KG_ONLY = "#999999"  # grey


def load_vis_graph(path: Path) -> Tuple[List[Dict,], List[Dict,]]:
    """Parse nodes/edges arrays out of an existing vis-network HTML export."""
    text = path.read_text()
    nodes_match = re.search(r"nodes\s*=\s*new vis.DataSet\((\[.*?\])\);", text, re.S)
    edges_match = re.search(r"edges\s*=\s*new vis.DataSet\((\[.*?\])\);", text, re.S)
    if not nodes_match or not edges_match:
        raise ValueError(f"Could not parse nodes/edges from {path}")
    nodes = json.loads(nodes_match.group(1))
    edges = json.loads(edges_match.group(1))
    return nodes, edges


def build_combined():
    kg_nodes, kg_edges = load_vis_graph(INPUT_KG)
    inf_nodes, inf_edges = load_vis_graph(INPUT_INFERRED)

    kg_map = {n["id"]: n for n in kg_nodes}
    inf_map = {n["id"]: n for n in inf_nodes}

    kg_ids = set(kg_map)
    inf_ids = set(inf_map)

    common_nodes = kg_ids & inf_ids
    inf_only_nodes = inf_ids - kg_ids
    kg_only_nodes = kg_ids - inf_ids

    combined_nodes = []
    for node_id in sorted(kg_ids | inf_ids):
        source = inf_map.get(node_id) or kg_map.get(node_id)
        node = {k: v for k, v in source.items() if k != "color"}
        if node_id in common_nodes:
            node["color"] = COLOR_COMMON
        elif node_id in inf_only_nodes:
            node["color"] = COLOR_INFERRED
        else:
            node["color"] = COLOR_KG_ONLY
        combined_nodes.append(node)

    kg_edge_keys = {(e["from"], e["to"], e.get("label", "")) for e in kg_edges}
    inf_edge_keys = {(e["from"], e["to"], e.get("label", "")) for e in inf_edges}

    common_edges = kg_edge_keys & inf_edge_keys
    inf_only_edges = inf_edge_keys - kg_edge_keys
    kg_only_edges = kg_edge_keys - inf_edge_keys

    def lookup(edges):
        by_key = {}
        for e in edges:
            key = (e["from"], e["to"], e.get("label", ""))
            by_key.setdefault(key, []).append(e)
        return by_key

    kg_lookup = lookup(kg_edges)
    inf_lookup = lookup(inf_edges)

    combined_edges = []
    inferred_edge_objs = []
    edge_id = 1

    for key in sorted(kg_edge_keys | inf_edge_keys):
        edge_src = inf_lookup.get(key, kg_lookup.get(key))[0]
        edge = {k: v for k, v in edge_src.items() if k != "color"}
        edge["id"] = edge_id
        edge_id += 1

        if key in common_edges:
            edge["color"] = {"color": COLOR_COMMON}
        elif key in inf_only_edges:
            edge["color"] = {"color": COLOR_INFERRED}
            inferred_edge_objs.append(edge.copy())
        else:
            edge["color"] = {"color": COLOR_KG_ONLY}
        combined_edges.append(edge)

    inferred_nodes = [n for n in combined_nodes if n["color"] == COLOR_INFERRED]

    html = render_html(
        nodes=combined_nodes,
        edges=combined_edges,
        inferred_nodes=inferred_nodes,
        inferred_edges=inferred_edge_objs,
    )
    OUTPUT.write_text(html)
    print(f"Wrote {OUTPUT} with {len(combined_nodes)} nodes and {len(combined_edges)} edges.")


def render_html(nodes, edges, inferred_nodes, inferred_edges):
    template = """<html>
    <head>
        <meta charset=\"utf-8\">
        <script src=\"lib/bindings/utils.js\"></script>
        <link rel=\"stylesheet\" href=\"https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.2/dist/dist/vis-network.min.css\" integrity=\"sha512-WgxfT5LWjfszlPHXRmBWHkV2eceiWTOBvrKCNbdgDYTHrT2AeLCGbF4sZlZw3UMN3WtL0tGUoIAKsu8mllg/XA==\" crossorigin=\"anonymous\" referrerpolicy=\"no-referrer\" />
        <script src=\"https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.2/dist/vis-network.min.js\" integrity=\"sha512-LnvoEWDFrqGHlHmDD2101OrLcbsfkrzoSpvtSQtxK3RMnRV0eOkhhBN2dXHKRrUU8p2DGRTk35n4O8nWSVe1mQ==\" crossorigin=\"anonymous\" referrerpolicy=\"no-referrer\"></script>
        <style>
             #mynetwork {{
                 width: 100%;
                 height: 900px;
                 background-color: #ffffff;
                 border: 1px solid lightgray;
                 position: relative;
             }}
             .toolbar {{display:flex; gap:12px; align-items:center; font-family: Arial, sans-serif; margin: 12px 0;}}
             .legend-item {{display:flex; align-items:center; gap:6px;}}
             .dot {{width:14px; height:14px; border-radius:50%;}}
             button {{padding:6px 12px; border:1px solid #ccc; border-radius:4px; background:#f5f5f5; cursor:pointer;}}
             button:hover {{background:#eaeaea;}}
        </style>
    </head>
    <body>
        <div class=\"toolbar\">
            <button id=\"toggleInferred\">Hide inferred</button>
            <div class=\"legend-item\"><span class=\"dot\" style=\"background:{color_common};\"></span><span>Common nodes/edges</span></div>
            <div class=\"legend-item\"><span class=\"dot\" style=\"background:{color_inferred};\"></span><span>Inferred-only (triples_IOF)</span></div>
            <div class=\"legend-item\"><span class=\"dot\" style=\"background:{color_kg};\"></span><span>KG-only (triples_IOF_KG)</span></div>
        </div>
        <div id=\"mynetwork\"></div>
        <script type=\"text/javascript\">
            const allNodes = {nodes_json};
            const allEdges = {edges_json};
            const inferredNodes = {inferred_nodes_json};
            const inferredNodeIds = inferredNodes.map(n => n.id);
            const inferredEdges = {inferred_edges_json};
            const inferredEdgeIds = inferredEdges.map(e => e.id);

            function drawGraph() {{
                const container = document.getElementById('mynetwork');
                const nodes = new vis.DataSet(allNodes);
                const edges = new vis.DataSet(allEdges);
                const data = {{nodes: nodes, edges: edges}};
                const options = {{
                    edges: {{smooth: {{enabled: true, type: 'dynamic'}}}},
                    interaction: {{hover: true, dragNodes: true}},
                    physics: {{
                        enabled: true,
                        repulsion: {{
                            centralGravity: 0.33,
                            damping: 0.95,
                            nodeDistance: 200,
                            springConstant: 0.1,
                            springLength: 110
                        }},
                        solver: 'repulsion',
                        stabilization: {{enabled: true, fit: true, iterations: 1000, updateInterval: 50}}
                    }}
                }};
                const network = new vis.Network(container, data, options);

                let inferredVisible = true;
                const btn = document.getElementById('toggleInferred');
                btn.addEventListener('click', () => {{
                    if (inferredVisible) {{
                        nodes.remove(inferredNodeIds);
                        edges.remove(inferredEdgeIds);
                        btn.textContent = 'Show inferred';
                    }} else {{
                        const currentNodeIds = nodes.getIds();
                        nodes.add(inferredNodes.filter(n => !currentNodeIds.includes(n.id)));
                        const currentEdgeIds = edges.getIds();
                        edges.add(inferredEdges.filter(e => !currentEdgeIds.includes(e.id)));
                        btn.textContent = 'Hide inferred';
                    }}
                    inferredVisible = !inferredVisible;
                }});

                return network;
            }}
            drawGraph();
        </script>
    </body>
</html>
"""

    return template.format(
        color_common=COLOR_COMMON,
        color_inferred=COLOR_INFERRED,
        color_kg=COLOR_KG_ONLY,
        nodes_json=json.dumps(nodes),
        edges_json=json.dumps(edges),
        inferred_nodes_json=json.dumps(inferred_nodes),
        inferred_edges_json=json.dumps(inferred_edges),
    )


if __name__ == "__main__":
    build_combined()
