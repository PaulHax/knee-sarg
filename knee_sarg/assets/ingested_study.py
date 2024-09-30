"""Injest staged study data into a collection's archive.

Staged study data can either come from the OAI or from the web browser portal.
"""

import os
import shutil
from pathlib import Path

import pandas as pd
import polars as pl
from dagster import asset, get_dagster_logger, Config, DynamicPartitionsDefinition
from slugify import slugify

from ..resources import CollectionTables, FileStorage

log = get_dagster_logger()


study_uid_partitions_def = DynamicPartitionsDefinition(name="study_uid")


class StagedStudyConfig(Config):
    """Configuration for the staged study asset."""

    collection_name: str
    uploader: str
    study_uid: str
    patient_id: str


def config_to_dataframe(config: StagedStudyConfig) -> pd.DataFrame:
    return pl.from_pandas(
        pd.DataFrame(
            [
                {
                    "collection_name": config.collection_name,
                    "uploader": config.uploader,
                    "patient_id": config.patient_id,
                    "study_uid": config.study_uid,
                }
            ]
        )
    )


def clean_column_name(name: str | int) -> str:
    return slugify(str(name).replace("%", "percent"), separator="_")


def convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts columns of a pandas dataframe with `date` in their name and contents that are of the form M/D/YYYY to YYYY-MM-DD.
    """
    date_columns = [col for col in df.columns if "date" in col.lower()]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], format="mixed", errors="coerce").dt.strftime(
            "%Y-%m-%d"
        )
    return df


def clean_empty_directories(root: Path, path: Path):
    """
    Recursively delete empty directories from the given path up to the root directory.

    :param root: The root directory to stop the recursion.
    :param path: The starting path to check for empty directories.
    """
    current_path = path

    while current_path != root:
        if not any(current_path.iterdir()):
            current_path.rmdir()
            current_path = current_path.parent
        else:
            break


@asset(
    partitions_def=study_uid_partitions_def, metadata={"partition_expr": "study_uid"}
)
def ingested_study(
    config: StagedStudyConfig,
    collection_tables: CollectionTables,
    file_storage: FileStorage,
) -> pl.DataFrame:
    """
    Ingested study data.
    """
    staged_study_path = (
        file_storage.staged_path
        / config.collection_name
        / config.uploader
        / config.patient_id
        / config.study_uid
    )

    patient = pd.read_json(staged_study_path / "patient.json", orient="index")
    patient = patient.rename(columns=clean_column_name)
    patient = convert_date_columns(patient)
    log.info(f"Ingesting study for patient: {config.patient_id}")
    collection_tables.insert_into_collection(
        config.collection_name, "patients", patient
    )

    study = pd.read_json(staged_study_path / "study.json", orient="rows")
    study = study.rename(columns=clean_column_name)
    study = convert_date_columns(study)
    collection_tables.insert_into_collection(config.collection_name, "studies", study)

    series = pd.read_json(staged_study_path / "series.json", orient="rows")
    series = series.rename(columns=clean_column_name)
    collection_tables.insert_into_collection(config.collection_name, "series", series)

    ingested_patient_path = file_storage.ingested_path / config.patient_id
    os.makedirs(ingested_patient_path, exist_ok=True)
    ingested_study_path = ingested_patient_path / config.study_uid
    if ingested_study_path.exists():
        shutil.rmtree(ingested_study_path)

    shutil.move(staged_study_path, ingested_patient_path)

    staged_patient_path = (
        file_storage.staged_path
        / config.collection_name
        / config.uploader
        / config.patient_id
    )
    clean_empty_directories(file_storage.staged_path, staged_patient_path)

    return config_to_dataframe(config)


@asset()
def collection_parquets(collection_tables: CollectionTables) -> None:
    """
    Export collection data to parquet files.
    """
    collection_tables.write_collection_parquets()
