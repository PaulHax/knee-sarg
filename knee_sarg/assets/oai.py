"""NIH Imaging Data Commons (OAI) dataset assets."""

from typing import List
from pathlib import Path
import shutil

import polars as pl
import pandas as pd
from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext,
    Config,
)
from pydantic import Field

from ..resources import (
    OAISampler,
    OaiPipeline,
    make_output_dir,
    OAI_COLLECTION_NAME,
)
from ..assets.ingested_study import (
    INGESTED_DIR,
    study_id_partitions_def,
    ingested_study,
)

log = get_dagster_logger()


class OaiPatientIds(Config):
    patient_id_file: str = Field(
        default_factory=lambda: "patient_small.json",
        description="JSON file with array of patient IDs",
    )


@asset()
def oai_patient_ids(
    oai_sampler: OAISampler,
    config: OaiPatientIds,
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


class ThicknessImages(Config):
    required_output_files: List[str] = Field(
        default_factory=lambda: ["thickness_FC.png", "thickness_TC.png"],
        description="List of required output files",
    )


@asset(
    partitions_def=study_id_partitions_def,
    metadata={"partition_expr": "study_id"},
    deps=[ingested_study],
    op_tags={"gpu": ""},
)
def cartilage_thickness(
    context: AssetExecutionContext, config: ThicknessImages, oai_pipeline: OaiPipeline
) -> pl.DataFrame:
    """
    Cartilage Thickness Images. Generates images for a series in data/collections/OAI_COLLECTION_NAME/patient_id/study_id/cartilage_thickness/series_id.
    """
    study_id = context.partition_key
    # gather images we want to run the pipeline on
    ingested_images_root: str = str(INGESTED_DIR)
    patient_dirs = [d for d in Path(ingested_images_root).iterdir() if d.is_dir()]
    study_dirs = [
        subdir
        for directory in patient_dirs
        for subdir in directory.iterdir()
        if subdir.is_dir()
    ]

    def get_first_file(dir: Path):
        return str(next((item for item in dir.iterdir() if not item.is_dir()), None))

    series_dirs = [
        {
            "patient_id": study_dir.parent.name,
            "study_id": study_dir.name,
            "series_id": series_dir.name,
            "study_dir": study_dir,
            "series_dir": series_dir,
            "image_path": get_first_file(series_dir),
        }
        for study_dir in study_dirs
        for series_dir in (study_dir / "nifti").iterdir()
        if series_dir.is_dir()
    ]

    study = next((item for item in series_dirs if item["study_id"] == study_id), None)
    if not study:
        raise Exception(
            f"Study with {study_id} not found in dir: {ingested_images_root}"
        )

    output_dir = make_output_dir(OAI_COLLECTION_NAME, study, "cartilage_thickness")
    computed_files_dir = output_dir / "cartilage_thickness"
    if computed_files_dir.exists():
        shutil.rmtree(computed_files_dir)

    oai_pipeline.run_pipeline(study["image_path"], str(computed_files_dir), study_id)

    # Check if specific files are in computed_files_dir
    missing_files = [
        file
        for file in config.required_output_files
        if not (computed_files_dir / file).exists()
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
                    "study_id": study["study_id"],
                    "series_id": study["series_id"],
                    "computed_files_dir": str(computed_files_dir),
                }
            ]
        ).astype(
            {
                "patient_id": "str",
                "study_id": "str",
                "series_id": "str",
                "computed_files_dir": "str",
            }
        )
    )
