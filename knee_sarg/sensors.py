import os

from dagster import (
    define_asset_job,
    sensor,
    RunRequest,
    RunConfig,
    DefaultSensorStatus,
    SensorResult,
)

from .assets.ingested_study import ingested_study, study_id_partitions_def
from .assets.oai import oai_samples, oai_patient_ids, cartilage_thickness
from .resources import STAGED_DIR


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
    [ingested_study, cartilage_thickness],
    description="Ingest a study into a collection and run analysis on it",
    partitions_def=study_id_partitions_def,
    tags={"job": "gpu"},
)


@sensor(job=ingest_and_analyze_study_job, default_status=DefaultSensorStatus.STOPPED)
def staged_study_sensor(context):
    """
    Sensor that triggers when a study is staged.
    """
    run_requests = []
    partitions_to_add = []
    for collection_name in os.listdir(STAGED_DIR):
        collection_path = STAGED_DIR / collection_name
        if not os.path.isdir(collection_path):
            continue
        for uploader in os.listdir(collection_path):
            uploader_path = collection_path / uploader
            for patient_id in os.listdir(uploader_path):
                patient_path = uploader_path / patient_id
                for study_id in os.listdir(patient_path):
                    run = RunRequest(
                        run_key=f"{collection_name}-{uploader}-{patient_id}-{study_id}",
                        partition_key=study_id,
                        run_config=RunConfig(
                            ops={
                                "ingested_study": {
                                    "config": {
                                        "collection_name": collection_name,
                                        "uploader": uploader,
                                        "study_id": study_id,
                                        "patient_id": patient_id,
                                    }
                                },
                            },
                        ),
                    )
                    run_requests.append(run)
                    partitions_to_add.append(study_id)
    return SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=[
            study_id_partitions_def.build_add_request(partitions_to_add)
        ],
    )
