from dagster import (
    define_asset_job,
)

from .assets.ingested_study import ingested_study, study_uid_partitions_def
from .assets.oai import (
    oai_samples,
    oai_patient_ids,
    cartilage_thickness,
    cartilage_thickness_runs,
)

stage_oai_samples_job = define_asset_job(
    "stage_oai_samples",
    [
        oai_patient_ids,
        oai_samples,
    ],
    description="Stages OAI samples",
)


ingest_and_analyze_study_job = define_asset_job(
    "ingest_and_analyze_study",
    [ingested_study, cartilage_thickness, cartilage_thickness_runs],
    description="Ingest a study into a collection and run analysis on it",
    partitions_def=study_uid_partitions_def,
    tags={"job": "gpu"},
)
