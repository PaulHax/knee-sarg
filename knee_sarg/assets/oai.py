"""NIH Imaging Data Commons (OAI) dataset assets."""

from typing import List
from pathlib import Path

import polars as pl
import pandas as pd
from dagster import (
    asset,
    asset_check,
    AssetCheckResult,
    get_dagster_logger,
    AssetExecutionContext,
    Config,
    DynamicPartitionsDefinition,
    EnvVar,
)
from dagster_duckdb import DuckDBResource
from pydantic import Field

from ..resources import (
    OAISampler,
    OaiPipeline,
    OAI_COLLECTION_NAME,
    CartilageThicknessTable,
    FileStorage,
)
from ..assets.ingested_study import (
    study_uid_partitions_def,
    ingested_study,
)

log = get_dagster_logger()

oai_patient_id_partitions_def = DynamicPartitionsDefinition(name="oai_patient_id")


class OaiPatientIdsConfig(Config):
    patient_id_file: str = Field(
        default_factory=lambda: "patient_small.json",
        description="JSON file with array of patient IDs",
    )


@asset()
def oai_patient_ids(
    oai_sampler: OAISampler,
    config: OaiPatientIdsConfig,
) -> pl.DataFrame:
    """
    OAI Patient IDs.
    """
    ids = oai_sampler.get_patient_ids(config.patient_id_file)

    return pl.from_pandas(
        pd.DataFrame(
            [
                {
                    "patient_id": patient_id,
                }
                for patient_id in ids
            ]
        ).astype(
            {
                "patient_id": "str",
            }
        )
    )


@asset()
def oai_samples(
    oai_sampler: OAISampler,
    oai_patient_ids: pl.DataFrame,
) -> pl.DataFrame:
    """
    OAI Samples. Samples are placed in data/staged/oai/dagster/.
    """
    patient_ids = oai_patient_ids["patient_id"]
    all_series = pd.concat(
        [oai_sampler.get_samples(patient_id) for patient_id in patient_ids],
        ignore_index=True,
    )
    return pl.from_pandas(all_series)


@asset(partitions_def=oai_patient_id_partitions_def)
def oai_sample(
    context: AssetExecutionContext,
    oai_sampler: OAISampler,
) -> None:
    """
    OAI Sample by patient_id partition. Samples are placed in data/staged/oai/dagster/.
    """
    patient_id = context.partition_key
    oai_sampler.get_samples(patient_id)


cartilage_thickness_code_version = EnvVar("CARTILAGE_THICKNESS_CODE_VERSION")


class ThicknessImages(Config):
    required_output_files: List[str] = Field(
        default_factory=lambda: ["thickness_FC.png", "thickness_TC.png"],
        description="List of required output files",
    )


@asset(
    deps=[ingested_study],
    partitions_def=study_uid_partitions_def,
    metadata={"partition_expr": "study_uid"},
    code_version=cartilage_thickness_code_version.get_value(),
)
def cartilage_thickness(
    context: AssetExecutionContext,
    config: ThicknessImages,
    duckdb: DuckDBResource,
    oai_pipeline: OaiPipeline,
    file_storage: FileStorage,
) -> pl.DataFrame:
    """
    Cartilage Thickness Images. Generates images for a series in data/collections/OAI_COLLECTION_NAME/patient_id/study_uid/cartilage_thickness/series_id.
    """
    study_uid = context.partition_key
    # get image to run the pipeline on
    ingested_images_root: Path = file_storage.ingested_path

    with duckdb.get_connection() as conn:
        cursor = conn.execute(
            f"SELECT * FROM {OAI_COLLECTION_NAME}_studies WHERE study_instance_uid = ?",
            (study_uid,),
        )
        row = cursor.fetchone()
        column_names = [desc[0] for desc in cursor.description]
        study = dict(zip(column_names, row))

    if not study:
        raise Exception(
            f"Study with uid {study_uid} not found in {OAI_COLLECTION_NAME}_studies table"
        )

    with duckdb.get_connection() as conn:
        cursor = conn.execute(
            f"""
            SELECT 
            *
            FROM {OAI_COLLECTION_NAME}_series
            WHERE study_instance_uid = ?
            """,
            (study_uid,),
        )
        column_names = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        series = dict(zip(column_names, row))

    study_dir_info = {
        "patient": study["patient_id"],
        "study": study["study_description"],
        "study_uid": study_uid,
    }
    output_dir = file_storage.make_output_dir(
        OAI_COLLECTION_NAME,
        study_dir_info,
        "cartilage_thickness",
        cartilage_thickness_code_version.get_value(),
    )

    image_path = (
        ingested_images_root
        / OAI_COLLECTION_NAME
        / study["patient_id"]
        / study_uid
        / "nifti"
        / series["series_instance_uid"]
        / "image.nii.gz"
    )

    oai_pipeline.run_pipeline(str(image_path), str(output_dir), study_uid)

    # Check if specific files are in computed_files_dir
    missing_files = [
        file
        for file in config.required_output_files
        if not (output_dir / file).exists()
    ]

    if missing_files:
        raise Exception(
            f"The following files are missing in computed_files_dir: {missing_files}"
        )

    return pl.from_pandas(
        pd.DataFrame(
            [
                {
                    "patient_id": study["patient_id"],
                    "study_uid": study_uid,
                    "series_id": series["series_instance_uid"],
                    "computed_files_dir": str(output_dir),
                }
            ]
        ).astype(
            {
                "patient_id": "str",
                "study_uid": "str",
                "series_id": "str",
                "computed_files_dir": "str",
            }
        )
    )


@asset(
    partitions_def=study_uid_partitions_def,
    metadata={"partition_expr": "study_uid"},
)
def cartilage_thickness_runs(
    cartilage_thickness: pl.DataFrame,
    cartilage_thickness_table: CartilageThicknessTable,
) -> pl.DataFrame:
    """
    Cartilage Thickness Run Statuses. Parquet table holding the status of cartilage thickness runs.
    Saved in data/collections/OAI_COLLECTION_NAME/cartilage_thickness_runs.parquet.
    """
    run = cartilage_thickness.to_pandas()
    cartilage_thickness_table.insert_run(run)
    cartilage_thickness_table.write_incremental_parquet(run)
    return cartilage_thickness


@asset_check(asset=cartilage_thickness_runs)
def check_cartilage_thickness_runs(
    config: ThicknessImages,
    cartilage_thickness_runs: pl.DataFrame,
):
    runs = cartilage_thickness_runs.to_pandas()

    study_uids_missing_files = [
        run.study_uid
        for run in runs.itertuples(index=False)
        if any(
            not (Path(run.computed_files_dir) / file).exists()
            for file in config.required_output_files
        )
    ]

    return AssetCheckResult(
        passed=len(study_uids_missing_files) == 0,
        metadata={"study_uids_missing_files": study_uids_missing_files},
    )
