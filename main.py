#!/usr/bin/env python3
"""
Orchestrator – runs the four-step Slack extraction pipeline:

    0. form-sync →  Google Form opt-outs  →  exclusion_rules.yaml
    1. extract   →  raw JSON in   slack_raw_data_<ts>/
    2. sanitize  →  clean text in  slack_processed_data_<ts>/
    3. chunk     →  split files in slack_output_sources_<ts>/

Usage
-----
    python main.py                          # full pipeline (all 4 steps)
    python main.py --skip-form-sync         # full pipeline, skip Google Form sync
    python main.py --step form-sync         # only sync exclusion rules from Google Form
    python main.py --step form-info         # print Google Form question IDs (setup helper)
    python main.py --step extract
    python main.py --step sanitize --input slack_raw_data_20260314_120000
    python main.py --step chunk    --input slack_processed_data_20260314_120000
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import yaml

from google_form_extractor import GoogleFormExtractor
from slack_extractor import extract_slack_data
from data_sanitizer import sanitize_data
from data_chunker import chunk_data

TOTAL_STEPS = 4


def _setup_logging() -> None:
    log_filename = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                          datefmt="%H:%M:%S")
    )

    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s  %(name)s  %(levelname)-8s  %(message)s")
    )

    root.addHandler(console)
    root.addHandler(file_handler)


def _load_config(path: str = "config.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Slack channel history extraction pipeline"
    )
    parser.add_argument(
        "--step",
        choices=["form-sync", "form-info", "extract", "sanitize", "chunk"],
        default=None,
        help="Run a single step instead of the full pipeline.",
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Input directory for the sanitize or chunk step "
             "(required when running a single step that needs prior output).",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to the configuration file (default: config.yaml).",
    )
    parser.add_argument(
        "--skip-form-sync",
        action="store_true",
        help="Skip the Google Form sync step during a full pipeline run.",
    )
    args = parser.parse_args()

    _setup_logging()
    logger = logging.getLogger("pipeline")

    config = _load_config(args.config)
    logger.info("Configuration loaded from %s", args.config)

    step = args.step

    # -- Standalone utility: print form structure and exit ---------------
    if step == "form-info":
        extractor = GoogleFormExtractor(config)
        extractor.print_form_structure()
        return

    # -- Step 0: Google Form → exclusion_rules.yaml ---------------------
    if step is None or step == "form-sync":
        if step is None and args.skip_form_sync:
            logger.info("Skipping Google Form sync (--skip-form-sync)")
        else:
            logger.info("=" * 60)
            logger.info(
                "STEP 1 / %d – Syncing exclusion rules from Google Form",
                TOTAL_STEPS,
            )
            logger.info("=" * 60)
            extractor = GoogleFormExtractor(config)
            extractor.fetch_and_update_exclusion_rules()
            if step == "form-sync":
                return

    # -- Step 1: Slack extraction ---------------------------------------
    if step is None or step == "extract":
        logger.info("=" * 60)
        logger.info(
            "STEP 2 / %d – Extracting Slack channel history", TOTAL_STEPS
        )
        logger.info("=" * 60)
        raw_dir = extract_slack_data(config)
        logger.info("Raw data directory: %s", raw_dir)
        if step == "extract":
            return
    else:
        raw_dir = None

    # -- Step 2: Sanitization -------------------------------------------
    if step is None or step == "sanitize":
        input_dir = args.input or raw_dir
        if not input_dir:
            logger.error(
                "--input is required when running the sanitize step alone"
            )
            sys.exit(1)
        if not Path(input_dir).is_dir():
            logger.error("Input directory does not exist: %s", input_dir)
            sys.exit(1)

        logger.info("=" * 60)
        logger.info(
            "STEP 3 / %d – Sanitizing and anonymizing data", TOTAL_STEPS
        )
        logger.info("=" * 60)
        processed_dir = sanitize_data(input_dir, config)
        logger.info("Processed data directory: %s", processed_dir)
        if step == "sanitize":
            return
    else:
        processed_dir = None

    # -- Step 3: Chunking -----------------------------------------------
    if step is None or step == "chunk":
        input_dir = args.input or processed_dir
        if not input_dir:
            logger.error(
                "--input is required when running the chunk step alone"
            )
            sys.exit(1)
        if not Path(input_dir).is_dir():
            logger.error("Input directory does not exist: %s", input_dir)
            sys.exit(1)

        logger.info("=" * 60)
        logger.info(
            "STEP 4 / %d – Splitting into token-bounded chunks", TOTAL_STEPS
        )
        logger.info("=" * 60)
        output_dir = chunk_data(input_dir, config)
        logger.info("Output sources directory: %s", output_dir)

    logger.info("=" * 60)
    logger.info("Pipeline finished successfully")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
