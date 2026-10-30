"""Shared bootstrap for the Glue Python shell jobs.

The stockstream package is shipped to Glue as a wheel via --extra-py-files.
"""

from __future__ import annotations

import sys


def args(*names: str) -> dict:
    try:
        from awsglue.utils import getResolvedOptions  # available inside Glue
        return getResolvedOptions(sys.argv, list(names))
    except ImportError:  # local run: --NAME value
        import argparse

        p = argparse.ArgumentParser()
        for n in names:
            p.add_argument(f"--{n}", required=n != "RUN_DATE")
        return vars(p.parse_args())
