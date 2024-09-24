import os

from dagster import (
    sensor,
    RunRequest,
    RunConfig,
    DefaultSensorStatus,
    SensorResult,
)

from .assets.ingested_study import study_uid_partitions_def
from .resources import STAGED_DIR
from .jobs import ingest_and_analyze_study_job


@sensor(job=ingest_and_analyze_study_job, default_status=DefaultSensorStatus.RUNNING)
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
                for study_uid in os.listdir(patient_path):
                    run = RunRequest(
                        run_key=f"{collection_name}-{uploader}-{patient_id}-{study_uid}",
                        partition_key=study_uid,
                        run_config=RunConfig(
                            ops={
                                "ingested_study": {
                                    "config": {
                                        "collection_name": collection_name,
                                        "uploader": uploader,
                                        "study_uid": study_uid,
                                        "patient_id": patient_id,
                                    }
                                },
                            },
                        ),
                    )
                    run_requests.append(run)
                    partitions_to_add.append(study_uid)
    return SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=[
            study_uid_partitions_def.build_add_request(partitions_to_add)
        ],
    )
