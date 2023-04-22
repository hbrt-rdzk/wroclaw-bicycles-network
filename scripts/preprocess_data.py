import os
import glob
import numpy as np
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

GEOLOCATION_COLUMNS = [
    "station_number",
    "station_name",
    "number_of_bikes_available",
    "number_of_bikes_racks",
    "latitude",
    "longitude",
]

GEOLOCATION_COLUMNS_TO_DROP = ["Współrzędne", "Numery dostępnych rowerów"]


def process_dataset(data: pd.DataFrame) -> pd.DataFrame:
    data.columns = COLUMNS
    indices = data[data.isin(["Poza stacją"]).any(axis=1)].index
    return data.drop(indices)


def process_geolocation_dataset(df_geolocation: pd.DataFrame) -> pd.DataFrame:
    latitude, longitude = np.vectorize(get_latitude_longitude)(
        df_geolocation["Współrzędne"]
    )
    df_geolocation["latitude"] = latitude
    df_geolocation["longitude"] = longitude
    df_geolocation.drop(GEOLOCATION_COLUMNS_TO_DROP, axis=1, inplace=True)
    df_geolocation.columns = GEOLOCATION_COLUMNS
    return df_geolocation


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


def get_latitude_longitude(geolocation: str):
    latitude, longitude = geolocation.split(", ")
    return float(latitude), float(longitude)


if __name__ == "__main__":
    # processing dataset
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

    # processing geolocation dataset
    geolocation_file_path = "dataset/raw/stacje_rowerowe.csv"
    df_geolocation = pd.read_csv(geolocation_file_path)
    df_geolocation_processed = process_geolocation_dataset(df_geolocation)
    df_geolocation_processed.to_csv(
        "dataset/processed/processed_geolocation_dataset.csv", index=False
    )
