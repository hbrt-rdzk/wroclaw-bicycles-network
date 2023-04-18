import os
import glob
import pandas as pd
from datetime import datetime


COLUMNS = [
    "uid",
    "bike_number",
    "start_time",
    "end_time",
    "rental_place",
    "return_place",
    "duration",
]


def process_dataset(data: pd.DataFrame) -> pd.DataFrame:
    data.columns = COLUMNS
    indices = data[data.isin(["Poza stacją"]).any(axis=1)].index
    return data.drop(indices)


def get_ride_durations_in_mins(
    start_times: pd.Series, end_times: pd.Series
) -> pd.Series:
    durations = []
    for start_time, end_time in zip(start_times, end_times):
        start_time_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end_time_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        duration = end_time_dt - start_time_dt
        durations.append(int(duration.seconds / 60))
    return durations


if __name__ == "__main__":
    datasets = []
    path = os.getcwd()
    files = glob.glob(os.path.join(path, "dataset/raw/historia_przejazdow_*"))
    for file in files:
        dataset = pd.read_csv(file)
        if "2019" in file:
            dataset = dataset.drop("Unnamed: 0", axis=1)
            dataset["duration"] = get_ride_durations_in_mins(
                dataset["start_time"], dataset["end_time"]
            )
        dataset = process_dataset(dataset)
        datasets.append(dataset)

    pd.concat(datasets).reset_index(drop=True).to_csv(
        "dataset/processed/processed_dataset.csv", index=False
    )
