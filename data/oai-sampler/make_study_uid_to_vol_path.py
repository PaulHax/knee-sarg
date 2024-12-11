# Got these messages/error when generating study_uid_to_vol_path.csv
# /mnt/cybertron/OAI/OAI96MonthImages/results/10.C.1/9005321/20120822/13915906/efs is a directory, skipping.
# 1.C.2/9220441/20051109/10784705 has no matching time point directory, skipping.
# 1.C.2/9220441/20051109/10784712 has no matching time point directory, skipping.
# 0.E.1/9332151/20051114/10780006 has no matching time point directory, skipping.
# 0.E.1/9332151/20051114/10780012 has no matching time point directory, skipping.

import os
from pathlib import Path
import pandas as pd
import itk

OAI_DATA_ROOT = "/mnt/cybertron/OAI"

months = [0, 12, 18, 24, 30, 36, 48, 72, 96]
time_point_dirs = {
    **{0: "OAIBaselineImages"},
    **{m: f"OAI{m}MonthImages" for m in months[1:]},
}


dess_file = "SEG_3D_DESS_all.csv"
dess = pd.read_csv(dess_file)


def find_parent_dir(sub_path: str) -> str:
    for month, dir in time_point_dirs.items():
        results = Path(OAI_DATA_ROOT) / dir / "results"
        if (results / sub_path).exists():
            return (month, dir)
    return None


records = []

# for each row in the DESS file,
# find the time_point_folder parent that has the Folder for the study row
for i, row in dess.iterrows():
    study_folder = row["Folder"]
    participant_id = row["ParticipantID"]
    time_point = find_parent_dir(study_folder)
    if not time_point:
        print(f"{study_folder} has no matching time point directory, skipping.")
        continue

    month, time_point_dir = time_point
    vol_path = Path(time_point_dir) / "results" / study_folder

    vol_abs = Path(OAI_DATA_ROOT) / vol_path

    first_file = vol_abs / os.listdir(vol_abs)[0]
    if first_file.is_dir():
        print(f"{first_file} is a directory, skipping.")
        continue

    frame_0 = itk.imread(first_file)
    meta = dict(frame_0)
    study_uid = meta["0020|000d"]
    study_description = meta.get("0008|1030", "").strip()

    records.append(
        {
            "patient_id": participant_id,
            "study_uid": study_uid,
            "vol_path": str(vol_path),
            "month": month,
            "study_description": study_description,
        },
    )


study_to_folder = pd.DataFrame(
    records,
    columns=["participant_id", "study_uid", "vol_path", "month", "study_description"],
)
study_to_folder.to_csv("study_uid_to_vol_path.csv", index=False)
