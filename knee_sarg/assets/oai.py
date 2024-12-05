"""NIH Imaging Data Commons (OAI) dataset assets."""

from typing import List
from pathlib import Path
import os
import shutil

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
    AssetIn,
)
from dagstermill import define_dagstermill_asset
from pydantic import Field

from scripts.cartilage_thickness_collection import THICKNESS_IMAGES

from ..resources import (
    OAISampler,
    OaiPipeline,
    CartilageThicknessTable,
    FileStorage,
    OAI_COLLECTION_NAME,
)
from ..assets.ingested_study import (
    study_uid_partitions_def,
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
    Reads OAI Patient IDs from a JSON file. Default file is DATA/oai-sampler/patient_small.json.
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
        default_factory=lambda: THICKNESS_IMAGES,
        description="List of required output files",
    )


@asset(
    partitions_def=study_uid_partitions_def,
    metadata={"partition_expr": "study_uid"},
    code_version=cartilage_thickness_code_version.get_value(),
)
def cartilage_thickness(
    context: AssetExecutionContext,
    config: ThicknessImages,
    oai_pipeline: OaiPipeline,
    file_storage: FileStorage,
    ingested_study_table: pl.DataFrame,
) -> pl.DataFrame:
    """
    Cartilage Thickness Images. Generates images for a series in data/collections/OAI_COLLECTION_NAME/patient_id/study_uid/cartilage_thickness/series_id.
    """
    study_uid = context.partition_key
    code_version = str(cartilage_thickness_code_version.get_value())
    if "oai/code-version" in context.run.tags:
        code_version = context.run.tags["oai/code-version"]
    override_src_directory = context.run.tags.get("oai/src-directory", None)

    # get image to run the pipeline on
    ingested_images_root: Path = file_storage.ingested_path

    collection_name, patient_id, study_uid, series_uid, study_description = (
        ingested_study_table.row(0)
    )

    study_dir_info = {
        "patient": patient_id,
        "study_description": study_description,
        "study_uid": study_uid,
    }
    output_dir = file_storage.make_output_dir(
        collection_name,
        study_dir_info,
        "cartilage_thickness",
        code_version,
    )

    image_path = (
        ingested_images_root
        / collection_name
        / patient_id
        / study_uid
        / "nifti"
        / series_uid
        / "image.nii.gz"
    )

    is_left = study_description.find("LEFT") > -1
    laterality = "left" if is_left else "right"

    oai_pipeline.run_pipeline(
        str(image_path), str(output_dir), laterality, study_uid, override_src_directory
    )

    # Check if specific files are in output_dir
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
                    "patient_id": patient_id,
                    "study_uid": study_uid,
                    "series_id": series_uid,
                    "computed_files_dir": str(output_dir),
                    "code_version": code_version,
                }
            ]
        ).astype(
            {
                "patient_id": "str",
                "study_uid": "str",
                "series_id": "str",
                "computed_files_dir": "str",
                "code_version": "str",
            }
        )
    )


@asset_check(asset=cartilage_thickness)
def has_current_code_version_output(
    config: ThicknessImages,
    cartilage_thickness: pl.DataFrame,
):
    """
    For each processed study_uid, check that it has output files with current code version.
    """
    code_version = str(cartilage_thickness_code_version.get_value())
    runs = cartilage_thickness.to_pandas()
    run_study_uids = runs["study_uid"].unique()

    study_uids_current_code_version = set(
        [
            run.study_uid
            for run in runs.itertuples(index=False)
            if run.code_version == code_version
            and all(
                (Path(run.computed_files_dir) / file).exists()
                for file in config.required_output_files
            )
        ]
    )

    stale_code_version_study_uids = [
        study_uid
        for study_uid in run_study_uids
        if study_uid not in study_uids_current_code_version
    ]

    return AssetCheckResult(
        passed=len(stale_code_version_study_uids) == 0,
        metadata={"stale_code_version_study_uids": stale_code_version_study_uids},
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
def has_image_files(
    config: ThicknessImages,
    cartilage_thickness_runs: pl.DataFrame,
):
    """
    Checks if collections folder has the output files for each run of cartilage_thickness.
    """
    runs = cartilage_thickness_runs.to_pandas()

    missing_directories = [
        {"study_uid": run.study_uid, "computed_files_dir": run.computed_files_dir}
        for run in runs.itertuples(index=False)
        if any(
            not (Path(run.computed_files_dir) / file).exists()
            for file in config.required_output_files
        )
    ]

    return AssetCheckResult(
        passed=len(missing_directories) == 0,
        metadata={"directories_with_missing_files": missing_directories},
    )


cartilage_thickness_images_notebook = define_dagstermill_asset(
    name="cartilage_thickness_images_notebook",
    notebook_path=str(
        Path(__file__).parent.parent.parent
        / "scripts"
        / "cartilage_thickness_images.ipynb"
    ),
    ins={"runs": AssetIn("cartilage_thickness_runs")},
)


class CollectImagesConfig(Config):
    out_dir: str = Field(
        default_factory=lambda: "all-images",
        description="Directory under collections/oai to put images in",
    )
    files_to_collect: List[str] = Field(
        default_factory=lambda: THICKNESS_IMAGES,
        description="Name of files in each case's cartilage_thickness directory that are to be copied into out_dir",
    )


@asset()
def collected_images(
    cartilage_thickness_runs: pl.DataFrame,
    config: CollectImagesConfig,
    file_storage: FileStorage,
) -> None:
    """
    All 2D images in one directory under collections/oai/all_images.
    """
    out_dir = file_storage.collections_path / OAI_COLLECTION_NAME / config.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    files_to_collect = config.files_to_collect

    for _, run in cartilage_thickness_runs.to_pandas().iterrows():
        patient_id = run["patient_id"]
        computed_dir = run["computed_files_dir"]
        study_description = computed_dir.split(os.sep)[-3].split("-")[0]

        # study description examples: "OAI^MR^12 MONTH^LEFT" "OAI^MR^24 MONTH^RIGHT" "OAI^MR^ENROLLMENT^LEFT"
        is_left = study_description.find("LEFT") > -1
        laterality = "left" if is_left else "right"

        month = study_description.split("^")[2]
        if month == "ENROLLMENT":
            month = "00 MONTH"

        for file_name in files_to_collect:
            src_path = os.path.join(computed_dir, file_name)
            if os.path.exists(src_path):
                name, extension = os.path.splitext(file_name)
                code_version = run["code_version"]
                new_filename = f"{code_version}-{name}-{patient_id}-{laterality}-{month}{extension}"
                dest_path = os.path.join(out_dir, new_filename)
                shutil.copy(src_path, dest_path)
            else:
                print(f"File not found: {src_path}")
