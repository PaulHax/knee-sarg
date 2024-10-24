import os
import subprocess
from pathlib import Path
import argparse
import pandas as pd
from dotenv import load_dotenv

DATA_ROOT = ""


def get_cartilage_thickness_runs_file_path():
    root_dir = os.getenv("FILE_STORAGE_ROOT", DATA_ROOT)
    oai_collection = Path(root_dir) / "collections" / "oai"
    runs_file = oai_collection / "cartilage_thickness_runs.parquet"
    return runs_file


def get_unique_study_uids(runs_file):
    runs = pd.read_parquet(runs_file)
    return runs["study_uid"].unique()


def comma_separated(study_uids):
    return ",".join(study_uids)


def run_backfill(study_count, tags):
    runs_file = get_cartilage_thickness_runs_file_path()
    study_uids = get_unique_study_uids(runs_file)[:study_count]
    partitions = comma_separated(study_uids)

    cmd = [
        "dagster",
        "job",
        "backfill",
        "-j",
        "cartilage_thickness_job",
        "--partitions",
        partitions,
    ]

    if tags:
        cmd.extend(["--tags", tags])

    subprocess.run(cmd)


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Run backfill for cartilage thickness job."
    )
    parser.add_argument(
        "--count",
        type=int,
        required=True,
        help="Number of study UIDs to include.",
    )
    parser.add_argument(
        "--tags",
        type=str,
        default="",
        help='Tags are dict, e.g., \'{"oai/code-version": "new-approach"}\'',
    )
    args = parser.parse_args()

    run_backfill(args.count, args.tags)
