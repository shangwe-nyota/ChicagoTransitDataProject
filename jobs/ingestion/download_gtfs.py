import os
import requests
import zipfile

GTFS_URL = "https://www.transitchicago.com/downloads/sch_data/google_transit.zip"
OUTPUT_DIR = "data/raw/gtfs"
ZIP_PATH = os.path.join(OUTPUT_DIR, "gtfs.zip")


def download_gtfs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Downloading GTFS data...")
    response = requests.get(GTFS_URL, timeout=60)
    response.raise_for_status()

    with open(ZIP_PATH, "wb") as f:
        f.write(response.content)

    print("Download complete.")
    print("Unzipping GTFS data...")

    with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
        zip_ref.extractall(OUTPUT_DIR)

    print("Unzip complete.")


if __name__ == "__main__":
    download_gtfs()
