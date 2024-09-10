import os
from pathlib import Path

from dagster import define_asset_job, sensor, RunRequest, RunConfig, DefaultSensorStatus

from .assets import ingested_study
from .resources import STAGED_DIR

injest_and_analyze_study_job = define_asset_job(
    "injest_and_analyze_study",
    [
        ingested_study.ingested_study,
        # more analysis assets here
    ],
    description="Injest a study into a collection and run analysis on it",
)

@sensor(job=injest_and_analyze_study_job, default_status=DefaultSensorStatus.STOPPED)
def staged_study_sensor(context):
    """
    Sensor that triggers when a study is staged.
    """
    for collection_name in os.listdir(STAGED_DIR):
        collection_path = STAGED_DIR / collection_name
        if not os.path.isdir(collection_path):
            continue
        for uploader in os.listdir(collection_path):
            uploader_path = collection_path / uploader
            for patient_id in os.listdir(uploader_path):
                patient_path = uploader_path / patient_id
                for study_id in os.listdir(patient_path):
                    yield RunRequest(
                        run_key=f"{collection_name}-{uploader}-{patient_id}-{study_id}",
                        run_config=RunConfig(
                            ops={"ingested_study": {"config": {"collection_name": collection_name, "uploader": uploader, "study_id": study_id , "patient_id": patient_id}}}
                        ),
                    )