"""
Nominis SLA measurement smoke test — runs the full report end-to-end.

Run:  pytest tests/test_nominis.py -v
   or: python -m pytest tests/ -v
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nominis.report import init_sla, run_report  # noqa: E402

load_dotenv()


def _require_token() -> None:
    if not os.getenv("BITQUERY_TOKEN"):
        pytest.skip("Set BITQUERY_TOKEN in .env")


def test_nominis_sla_report(capsys):
    """Generate SLA fulfillment report (always passes — inspect % in output)."""
    _require_token()
    init_sla([])
    asyncio.run(run_report())
    out = capsys.readouterr().out
    assert "NOMICS SLA FULFILLMENT REPORT" in out


if __name__ == "__main__":
    from nominis.report import main

    main()
