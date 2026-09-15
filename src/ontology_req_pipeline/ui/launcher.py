"""Console launcher for the Streamlit application."""

from __future__ import annotations

import sys
from pathlib import Path


def main(port: int = 8501) -> None:
    try:
        from streamlit.web import cli as streamlit_cli
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise ImportError("Install UI dependencies with `pip install -e .[ui]`") from exc
    app_path = Path(__file__).with_name("app.py")
    sys.argv = [
        "streamlit",
        "run",
        str(app_path),
        "--server.port",
        str(port),
        "--browser.gatherUsageStats",
        "false",
    ]
    raise SystemExit(streamlit_cli.main())


if __name__ == "__main__":
    main()
