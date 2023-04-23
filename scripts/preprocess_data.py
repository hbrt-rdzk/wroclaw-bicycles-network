import json
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
    "coordinates",
]

GEOLOCATION_COLUMNS_TO_DROP = ["Współrzędne", "Numery dostępnych rowerów"]


def process_dataset(
    data: pd.DataFrame, unique_station_names: list[str]
) -> pd.DataFrame:
    global total_rides
    global bikes_outside_station

    data.columns = COLUMNS

    for column_name in data.columns:
        if data[column_name].dtype == "object":
            data[column_name] = data[column_name].str.replace("\xa0", " ")
            data[column_name] = data[column_name].str.rstrip()
            data[column_name] = data[column_name].str.rstrip(",")

    number_of_bike_rides = len(data)
    total_rides += number_of_bike_rides

    indices = data[data.isin(["Poza stacją"]).any(axis=1)].index
    data.drop(indices, inplace=True)

    filtered_number_of_bikes = len(data)
    bikes_outside_station += number_of_bike_rides - filtered_number_of_bikes

    data = data[
        data["return_place"].isin(unique_station_names)
        & data["rental_place"].isin(unique_station_names)
    ]

    return data


def process_geolocation_dataset(
    df_geolocation: pd.DataFrame,
) -> tuple[pd.DataFrame, list]:
    latitude, longitude = np.vectorize(get_latitude_longitude)(
        df_geolocation["Współrzędne"]
    )
    df_geolocation["latitude"] = latitude
    df_geolocation["longitude"] = longitude
    df_geolocation["coordinates"] = df_geolocation.apply(
        lambda row: (row["longitude"], row["latitude"]), axis=1
    )

    df_geolocation.drop(GEOLOCATION_COLUMNS_TO_DROP, axis=1, inplace=True)
    df_geolocation.columns = GEOLOCATION_COLUMNS

    unique_station_names = list(df_geolocation["station_name"].unique())

    return df_geolocation, unique_station_names


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
    # processing geolocation dataset
    geolocation_file_path = "dataset/raw/stacje_rowerowe.csv"
    df_geolocation = pd.read_csv(geolocation_file_path)
    df_geolocation_processed, unique_station_names = process_geolocation_dataset(
        df_geolocation
    )
    df_geolocation_processed.to_csv(
        "dataset/processed/processed_geolocation_dataset.csv", index=False
    )

    # processing dataset
    datasets = []
    path = os.getcwd()
    files = glob.glob(os.path.join(path, "dataset/raw/historia_przejazdow_*"))

    total_rides = 0
    bikes_outside_station = 0

    for file in files:
        dataset = pd.read_csv(file)
        if "2019" in file:
            dataset = dataset.drop("Unnamed: 0", axis=1)
            dataset["duration"] = get_ride_durations_in_mins(
                dataset["start_time"], dataset["end_time"]
            )
        dataset = process_dataset(dataset, unique_station_names)
        datasets.append(dataset)

    outside_to_total_ratio = round(bikes_outside_station / total_rides, 2)

    bikes_left_outside_stats = {
        "total_rides": total_rides,
        "bikes_outside_station": bikes_outside_station,
        "outside_to_total_ratio": outside_to_total_ratio,
    }

    with open("metrics/bikes_left_outside_stats.json", "w") as file:
        json.dump(bikes_left_outside_stats, file, indent=4)

    pd.concat(datasets).reset_index(drop=True).to_csv(
        "dataset/processed/processed_dataset.csv", index=False
    )
