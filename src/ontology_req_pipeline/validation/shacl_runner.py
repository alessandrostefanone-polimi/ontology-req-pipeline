"""SHACL validation runner."""

from __future__ import annotations

from typing import Optional, Tuple

import rdflib
from pyshacl import validate


def validate_graph(
    graph: rdflib.Graph,
    shapes_graph: rdflib.Graph,
    ont_graph: Optional[rdflib.Graph] = None,
    *,
    inference: str = "none",
    advanced: bool = False,
    allow_infos: bool = True,
    allow_warnings: bool = True,
) -> Tuple[bool, str]:
    """Validate a data graph against a SHACL shapes graph."""
    conforms, _, report_text = validate(
        data_graph=graph,
        shacl_graph=shapes_graph,
        ont_graph=ont_graph,
        inference=inference,
        abort_on_first=False,
        meta_shacl=False,
        advanced=advanced,
        allow_infos=allow_infos,
        allow_warnings=allow_warnings,
        js=False,
        debug=False,
    )
    return bool(conforms), str(report_text)
