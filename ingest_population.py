# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Ingest Population Data
"""Land the Rearc Data Quest Data USA population response in a Unity Catalog Volume.

Set POPULATION_VOLUME_PATH to the raw landing directory used by bronze.py.
The script is rerunnable: identical API content does not create another raw file.
"""

# Import hashlib for computing the SHA-256 checksum of the API response
import hashlib
# Import json for parsing and validating the Data USA JSON response
import json
# Import datetime and timezone for generating UTC timestamps in manifest records
from datetime import datetime, timezone
# Import Path for cross-platform path manipulation of volume destination files
from pathlib import Path
# Import Request and urlopen from urllib for making HTTP GET requests to the Data USA API
from urllib.request import Request, urlopen

# Define the Data USA API endpoint URL for total population by year and nation
API_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?"
    "cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
)

# Change this to match your Unity Catalog Volume.
# Set the destination directory path inside the Unity Catalog volume where raw population JSON files will be stored
POPULATION_VOLUME_PATH = "/Volumes/quest/bls/bls_raw/raw/"

# Define the User-Agent string that identifies this script to the Data USA API, including a contact email
USER_AGENT = "rearc-data-quest/1.0 (contact: sleff42@gmail.com)"
# Set the HTTP request timeout in seconds to avoid hanging on unresponsive servers
TIMEOUT_SECONDS = 60


# Fetch the population data from the Data USA API and validate the response structure before returning the raw bytes
def fetch_population() -> bytes:
    # Build an HTTP Request object with custom headers for User-Agent and Accept
    request = Request(
        API_URL,
        headers={
            "User-Agent": USER_AGENT,       # Identify the script to the API server
            "Accept": "application/json",   # Request JSON-formatted response data
        },
    )
    # Open the URL with the specified timeout, using a context manager to ensure the connection is closed
    with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        # Check if the HTTP status code is not 200 OK, and raise an error if the request failed
        if response.status != 200:
            raise RuntimeError(f"Data USA request failed with HTTP {response.status}")
        # Read the entire response body into a bytes object
        body = response.read()

    # Validate the response before landing it.
    # Parse the raw response bytes into a Python dictionary using json.loads
    payload = json.loads(body)
    # Check that the top-level 'data' key exists and is a list; raise an error if the structure is unexpected
    if "data" not in payload or not isinstance(payload["data"], list):
        raise ValueError("Unexpected Data USA response: top-level 'data' array is missing")
    # Check that the data array is not empty; raise an error if no records were returned
    if not payload["data"]:
        raise ValueError("Data USA returned an empty data array")

    # Define the set of required fields that must be present in each data record
    required = {"Year", "Nation", "Population"}
    # Compute the set difference to find any required fields missing from the first data record
    missing = required - set(payload["data"][0])
    # If any required fields are missing, raise a ValueError listing them
    if missing:
        raise ValueError(f"Data USA response is missing required fields: {sorted(missing)}")

    # Return the original raw response bytes (not the parsed dict) for landing as an immutable source file
    return body


# Download, hash, and land the population data in the Unity Catalog volume; return the destination file path
def ingest_population() -> str:
    # Fetch the validated raw JSON response bytes from the Data USA API
    body = fetch_population()
    # Compute the SHA-256 hash of the response body for content addressing and deduplication
    content_hash = hashlib.sha256(body).hexdigest()

    # Create a Path object for the destination directory inside the Unity Catalog volume
    destination_dir = Path(POPULATION_VOLUME_PATH)
    # Create the destination directory (and any parent directories) if it does not already exist
    destination_dir.mkdir(parents=True, exist_ok=True)

    # Content-addressed name makes repeat ingestion idempotent while retaining
    # a raw, immutable source response when the provider changes its content.
    # Construct a content-addressed destination filename using the SHA-256 hash (currently overridden by the next line)
    destination = destination_dir / f"population_{content_hash}.json"
    # Override the content-addressed name with a fixed filename so the latest data always lands at a stable path
    destination = destination_dir / f"population.json"

    # Check if the destination file already exists; if so, skip the write and return the existing path
    if destination.exists():
        # Print a message indicating the data is unchanged and the existing file is retained
        print(f"Population data unchanged; existing raw file retained: {destination}")
        # Return the path string of the existing file
        return str(destination)

    # Atomic replace prevents readers from seeing a partially written JSON file.
    # Create a temporary file path by replacing the .json suffix with .json.tmp
    temp = destination.with_suffix(".json.tmp")
    # Write the full response body to the temporary file in binary mode
    temp.write_bytes(body)
    # Atomically rename (replace) the temp file to the final destination, ensuring no partial reads
    temp.replace(destination)

    # Build a manifest record dictionary with metadata about this ingestion
    manifest_record = {
        "source_url": API_URL,                        # The API endpoint URL that was queried
        "landed_file": str(destination),              # The path where the raw file was saved
        "sha256": content_hash,                       # The SHA-256 hash of the response content
        "bytes": len(body),                           # The total size of the response in bytes
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),  # ISO-formatted UTC timestamp of ingestion
    }
    # Construct the path for the JSONL manifest file that tracks all ingestion runs
    manifest = destination_dir / "_population_manifest.jsonl"
    # Open the manifest file in append mode with UTF-8 encoding to add a new line per ingestion
    with manifest.open("a", encoding="utf-8") as f:
        # Write the manifest record as a compact JSON object on a single line followed by a newline
        f.write(json.dumps(manifest_record, separators=(",", ":")) + "\n")

    # Print the destination path where the population data was landed
    print(f"Population data landed: {destination}")
    # Print the SHA-256 hash of the landed content for verification
    print(f"SHA-256: {content_hash}")
    # Return the destination file path as a string to the caller
    return str(destination)


# When this script is executed directly (not imported as a module), run the ingestion function
if __name__ == "__main__":
    # Call the main ingestion function to fetch and land the population data
    ingest_population()