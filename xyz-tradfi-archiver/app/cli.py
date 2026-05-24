from __future__ import annotations

import argparse
import json
import logging
import sys

from app.config import Settings
from app.logging_config import configure_logging
from app.storage.metadata_store import MetadataStore
from app.workers.recorder import run_recorder
from app.workers.validator import validate_loop, validate_once
from app.workers.writer import run_writer

logger = logging.getLogger("xyz_archiver.cli")


def main(argv: list[str] | None = None) -> int:
    configure_logging()

    parser = argparse.ArgumentParser(prog="xyz-archiver")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("record")
    subparsers.add_parser("write")
    subparsers.add_parser("validate-once")
    subparsers.add_parser("validate-loop")
    subparsers.add_parser("inspect")

    args = parser.parse_args(argv)
    settings = Settings.from_env()

    logger.info(
        "starting command=%s run_id=%s state_dir=%s bucket=%s endpoint=%s dex=%s",
        args.command,
        settings.archiver_run_id,
        settings.archiver_state_dir,
        settings.archive_bucket,
        settings.archive_s3_endpoint,
        settings.hyperliquid_dex,
    )

    if args.command == "record":
        run_recorder(settings)
        return 0

    if args.command == "write":
        run_writer(settings)
        return 0

    if args.command == "validate-once":
        print(json.dumps(validate_once(settings), indent=2, sort_keys=True), flush=True)
        return 0

    if args.command == "validate-loop":
        validate_loop(settings)
        return 0

    if args.command == "inspect":
        metadata_store = MetadataStore(settings.metadata_db_path)
        payload = {
            "metadata_db_path": str(settings.metadata_db_path),
            "object_count": metadata_store.object_count(),
            "latest_objects": metadata_store.latest_objects(limit=20),
            "latest_health": metadata_store.latest_health(limit=20),
        }
        print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))