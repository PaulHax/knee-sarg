from typing import TypedDict

import os
import shutil
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

DATA_DIR = "data"  # if no env var, default directory where collections directory lives


class StudyInfo(TypedDict):
    patient_id: str
    study_description: str
    study_uid: str


THICKNESS_IMAGES = ["FC_thickness.png", "TC_thickness.png"]

load_dotenv()


class FilePaths:
    def __init__(
        self,
        root_dir: str = str(DATA_DIR),
        staged_dir: str = "",
        ingested_dir: str = "",
        collections_dir: str = "",
    ):
        root = Path(root_dir)
        self._staged_path = Path(staged_dir) if staged_dir else root / "staged"
        self._ingested_path = Path(ingested_dir) if ingested_dir else root / "ingested"
        self._collections_path = (
            Path(collections_dir) if collections_dir else root / "collections"
        )

    @property
    def staged_path(self) -> Path:
        return self._staged_path

    @property
    def ingested_path(self) -> Path:
        return self._ingested_path

    @property
    def collections_path(self) -> Path:
        return self._collections_path

    def get_collection_path(self, collection: str) -> Path:
        return self.collections_path / collection

    def ensure_collection_dir(self, collection: str):
        collection_dir = self.get_collection_path(collection)
        if not collection_dir.exists():
            collection_dir.mkdir(parents=True)
        return collection_dir

    def get_study_collection_dir(
        self,
        collection: str,
        study_info: StudyInfo,
    ) -> Path:
        patient, study_description, study_uid = (
            study_info["patient_id"],
            study_info["study_description"],
            study_info["study_uid"],
        )
        return (
            self.get_collection_path(collection)
            / patient
            / f"{study_description}-{study_uid}"
        )

    def get_output_dir(
        self,
        collection: str,
        study_info: StudyInfo,
        analysis_name: str,
        code_version: str = "None",
    ) -> Path:
        study_dir = self.get_study_collection_dir(collection, study_info)
        output_dir = study_dir / analysis_name / code_version
        return output_dir

    def make_output_dir(
        self,
        collection: str,
        dir_info: StudyInfo,
        analysis_name: str,
        code_version: str = "None",
    ) -> Path:
        output_dir = self.get_output_dir(
            collection, dir_info, analysis_name, code_version
        )
        if not output_dir.exists():
            output_dir.mkdir(parents=True)
        else:
            # clean out the directory
            for item in output_dir.iterdir():
                if item.is_file() or item.is_symlink():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
        return output_dir


def get_root_dir():
    return os.getenv("FILE_STORAGE_ROOT", DATA_DIR)


def get_oai_collection_dir():
    root_dir = get_root_dir()
    return Path(root_dir) / "collections" / "oai"


def get_cartilage_thickness_runs_file_path():
    oai_collection = get_oai_collection_dir()
    runs_file = oai_collection / "cartilage_thickness_runs.parquet"
    return runs_file


def get_runs():
    runs_file = get_cartilage_thickness_runs_file_path()
    return pd.read_parquet(runs_file)


def get_run(study_uid: str, code_version: str = ""):
    """Gets most recent run"""
    runs = get_runs()

    if code_version:
        matched_row = runs[
            (runs["study_uid"] == study_uid) & (runs["code_version"] == code_version)
        ].iloc[-1]
    else:
        matched_row = runs[(runs["study_uid"] == study_uid)].iloc[-1]
    return matched_row


def get_computed_files_dir(study_uid: str, code_version: str = ""):
    """Gets most recent run"""
    run = get_run(study_uid, code_version)
    return run["computed_files_dir"]


def get_patient_id(study_uid: str):
    import duckdb

    db_file = str(Path(DATA_DIR) / "database.duckdb")
    conn = duckdb.connect(db_file)
    query = f"SELECT patient_id FROM oai_studies WHERE study_uid = '{study_uid}'"
    result = conn.execute(query)
    return result.fetchone()[0]


if __name__ == "__main__":
    print(get_computed_files_dir("1.3.12.2.1107.5.2.13.20576.4.0.9005364411762704"))
