from typing import Dict, Any
import os
from pathlib import Path
import pandas as pd

DATA_DIR = "data"  # if no env var, default directory where collections directory lives

StudyInfo = Dict[str, Any]


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

    def get_output_dir(
        self,
        collection: str,
        dir_info: StudyInfo,
        analysis_name: str,
        code_version: str = "undefined",
    ) -> Path:
        patient, study_description, study_uid = (
            dir_info["patient"],
            dir_info["study_description"],
            dir_info["study_uid"],
        )
        study_dir = (
            self.collections_path
            / collection
            / patient
            / f"{study_description}-{study_uid}"
        )
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
            output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir


def get_cartilage_thickness_runs_file_path():
    root_dir = os.getenv("FILE_STORAGE_ROOT", DATA_DIR)
    oai_collection = Path(root_dir) / "collections" / "oai"
    runs_file = oai_collection / "cartilage_thickness_runs.parquet"
    return runs_file


def get_output_dir(study_uid: str, code_version: str = ""):
    runs_file = get_cartilage_thickness_runs_file_path()
    runs = pd.read_parquet(runs_file)

    if code_version:
        matched_row = runs[
            (runs["study_uid"] == study_uid) & (runs["code_version"] == code_version)
        ].iloc[-1]
    else:
        matched_row = runs[(runs["study_uid"] == study_uid)].iloc[-1]

    output_dir = matched_row["output_dir"]
    return output_dir