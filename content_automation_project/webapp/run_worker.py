#!/usr/bin/env python3
"""Celery worker process.

Equivalent CLI (preferred in Docker Compose):

  celery -A webapp.celery_app worker --loglevel=info

From project root with PYTHONPATH=. :

  python -m webapp.run_worker
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    from webapp.celery_app import celery_app

    extra = sys.argv[1:]
    argv = ["worker", "--loglevel=info"] + extra
    celery_app.worker_main(argv)


if __name__ == "__main__":
    main()
