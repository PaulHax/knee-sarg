import os
from pathlib import Path
import json
import itk


def dicom_to_ingested(vol_folder: Path, out_dir: Path, patient_id: str) -> None:
    frame_0 = itk.imread(vol_folder / os.listdir(vol_folder)[0])
    meta = dict(frame_0)
    image = itk.imread(str(vol_folder))

    study_uid = meta["0020|000d"]
    series_uid = meta["0020|000e"]

    study_date = meta.get("0008|0020", "00000000")
    study_date = f"{study_date[4:6]}-{study_date[6:8]}-{study_date[:4]}"
    study_description = meta.get("0008|1030", "").strip()
    series_number = meta.get("0020|0011", "0")
    series_number = int(series_number)
    modality = meta.get("0008|0060", "").strip()
    body_part_examined = meta.get("0018|0015", "")
    series_description = meta.get("0008|103e", "").strip()

    study = {
        "patient_id": patient_id,
        "study_uid": study_uid,
        "study_date": study_date,
        "study_description": study_description,
    }

    series = {
        "patient_id": patient_id,
        "study_uid": study_uid,
        "series_uid": series_uid,
        "series_number": series_number,
        "modality": modality,
        "body_part_examined": body_part_examined,
        "series_description": series_description,
    }

    os.makedirs(out_dir, exist_ok=True)

    with open(out_dir / "study.json", "w") as f:
        json.dump(study, f)

    with open(out_dir / "series.json", "w") as f:
        json.dump(series, f)

    nifti_path = out_dir / "nifti" / series_uid
    os.makedirs(nifti_path, exist_ok=True)
    itk.imwrite(image, nifti_path / "image.nii.gz")
