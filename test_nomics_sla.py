"""
Nomics SLA measurement — prints fulfillment % per metric (no pass/fail).

Run:  python sla_report.py
   or: python test_nomics_sla.py
   or: pytest test_nomics_sla.py -v
"""

import os

import pytest
from dotenv import load_dotenv

import asyncio

from sla_report import init_sla, run_report

load_dotenv()


def _require_token() -> None:
    if not os.getenv("BITQUERY_TOKEN"):
        pytest.skip("Set BITQUERY_TOKEN in .env")


def test_nomics_sla_report(capsys):
    """Generate SLA fulfillment report (always passes — inspect % in output)."""
    _require_token()
    init_sla([])
    asyncio.run(run_report())
    out = capsys.readouterr().out
    assert "NOMICS SLA FULFILLMENT REPORT" in out


if __name__ == "__main__":
    from sla_report import main

    main()
