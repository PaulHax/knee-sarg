import pandas as pd
import json

study_to_vol_path = pd.read_csv("study_uid_to_vol_path.csv", dtype=str)


studies = study_to_vol_path.sample(n=30, random_state=1)

# studies = studies[
#     studies["study_description"].str.contains("left", case=False, na=False)
# ]

# patient_ids_to_keep = ["9000798", "9007827"]
# studies = studies[studies["patient_id"].isin(patient_ids_to_keep)]


study_uids = studies["study_uid"]
with open("study_uids_to_run.json", "w") as json_file:
    json.dump(study_uids.tolist(), json_file)
