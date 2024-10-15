import pandas as pd
import json
import os


dess_file = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "SEG_3D_DESS_all.csv"
)
dess_df = pd.read_csv(dess_file)

p_ids = dess_df["ParticipantID"]
p_ids_set = set(map(str, p_ids))

with open("all_patient_ids.json", "w") as fp:
    json.dump(list(p_ids_set), fp)
