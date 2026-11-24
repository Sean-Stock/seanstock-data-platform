#!/usr/bin/env python3
"""Single-entrypoint runner executing extract -> DuckDB transform & DQ checks."""

from __future__ import annotations

import sys

from extract_uk_grid import main as extract_main
from build_gold_marts import main as transform_main


def main() -> int:
    rc = extract_main()
    if rc != 0:
        return rc
    return transform_main()


if __name__ == "__main__":
    sys.exit(main())
