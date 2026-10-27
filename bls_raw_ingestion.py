# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # BLS Raw File Ingestion
# MAGIC Dynamically discovers all files beneath a BLS bulk-download directory, sends a contact-bearing `User-Agent`, lands raw bytes in a Unity Catalog Volume, and maintains Delta manifest/run tables for incremental and idempotent processing.

# COMMAND ----------

# Configuration
# This cell defines all configurable parameters for the BLS raw ingestion pipeline via Databricks widgets.

# Create a text widget for the BLS source directory URL with a default value
dbutils.widgets.text("source_url", "https://download.bls.gov/pub/time.series/pr/", "BLS source directory")
# Create a text widget for the Unity Catalog catalog name
dbutils.widgets.text("catalog", "quest", "Catalog")
# Create a text widget for the Unity Catalog schema name
dbutils.widgets.text("schema", "bls", "Schema")
# Create a text widget for the Unity Catalog volume name where raw files will be stored
dbutils.widgets.text("volume", "bls_raw", "Volume")
# Create a text widget for the contact email required by BLS for identification
dbutils.widgets.text("contact_email", "sleff42@gmail.com", "BLS contact email")

# Retrieve the source URL from the widget, strip trailing slashes, and append one trailing slash for consistent URL joining
SOURCE_URL = dbutils.widgets.get("source_url").rstrip("/") + "/"
# Retrieve the catalog name from the widget and strip leading/trailing whitespace
CATALOG = dbutils.widgets.get("catalog").strip()
# Retrieve the schema name from the widget and strip leading/trailing whitespace
SCHEMA = dbutils.widgets.get("schema").strip()
# Retrieve the volume name from the widget and strip leading/trailing whitespace
VOLUME = dbutils.widgets.get("volume").strip()
# Retrieve the contact email from the widget and strip leading/trailing whitespace
CONTACT_EMAIL = dbutils.widgets.get("contact_email").strip()

# Validate that a real contact email was provided; block execution if missing, still at placeholder, or lacks an @ sign
if not CONTACT_EMAIL or CONTACT_EMAIL == "your.email@company.com" or "@" not in CONTACT_EMAIL:
    # Raise a ValueError to halt execution before any HTTP requests are made without proper identification
    raise ValueError("Set contact_email to a real, monitored email address before running this notebook.")

# Construct the root path of the Unity Catalog volume where raw files will be stored
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
# Construct the subdirectory path within the volume for landing raw downloaded files
RAW_ROOT = f"{VOLUME_ROOT}/raw"
# Construct the fully qualified Delta table name for the ingestion manifest table
MANIFEST_TABLE_NAME = f"{CATALOG}.{SCHEMA}.bls_ingestion_manifest"
# Construct the fully qualified Delta table name for the ingestion run log table
RUN_TABLE_NAME = f"{CATALOG}.{SCHEMA}.bls_ingestion_runs"
# Construct the SQL-qualified (backtick-quoted) manifest table name for use in SQL statements
MANIFEST_TABLE_SQL = f"`{CATALOG}`.`{SCHEMA}`.`bls_ingestion_manifest`"
# Construct the SQL-qualified (backtick-quoted) run table name for use in SQL statements
RUN_TABLE_SQL = f"`{CATALOG}`.`{SCHEMA}`.`bls_ingestion_runs`"

# Print the configured source URL for visibility in the notebook output
print(f"Source: {SOURCE_URL}")
# Print the configured destination path for visibility in the notebook output
print(f"Destination: {RAW_ROOT}")
# Print the configured contact email for visibility in the notebook output
print(f"Contact: {CONTACT_EMAIL}")

# COMMAND ----------

# Create Unity Catalog objects and Delta control tables
# This cell ensures the catalog schema, volume, and Delta control tables exist before ingestion begins.

# Create the Unity Catalog schema if it does not already exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`bls_bronze`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`bls_silver`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`bls_gold`")

# Create the Unity Catalog volume to hold raw downloaded BLS files if it does not already exist
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`
COMMENT 'Raw files downloaded from BLS'
""")

# Create the manifest Delta table to track every discovered file's metadata and ingestion status
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE_SQL} (
  source_root STRING NOT NULL,          -- The root BLS directory URL that files were discovered from
  source_url STRING NOT NULL,           -- The full URL of the source file on the BLS server
  relative_path STRING NOT NULL,        -- The file path relative to the source root, used as a unique key
  destination_path STRING NOT NULL,     -- The full path where the file is stored in the Unity Catalog volume
  source_etag STRING,                   -- The ETag header from the BLS server, used for change detection
  source_last_modified STRING,          -- The Last-Modified header from the BLS server, used for change detection
  source_content_length BIGINT,         -- The Content-Length header from the BLS server, used for change detection
  sha256 STRING,                        -- The SHA-256 hash of the downloaded file content, used for deduplication
  file_size BIGINT,                     -- The actual byte count of the downloaded file
  source_status STRING,                 -- Whether the file is ACTIVE or REMOVED on the BLS server
  ingestion_status STRING,              -- Whether ingestion is COMPLETE or FAILED for this file
  first_seen_at TIMESTAMP,              -- Timestamp of the first run that discovered this file
  last_seen_at TIMESTAMP,               -- Timestamp of the most recent run that saw this file
  ingested_at TIMESTAMP,                -- Timestamp of the most recent successful download
  removed_at TIMESTAMP,                 -- Timestamp when the file was marked as removed upstream
  last_run_id STRING,                   -- UUID of the most recent ingestion run that processed this file
  error_message STRING                 -- Error details if the last ingestion attempt failed
) USING DELTA
""")

# Create the run log Delta table to record a summary row for each ingestion run
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RUN_TABLE_SQL} (
  run_id STRING NOT NULL,               -- Unique UUID identifying this ingestion run
  source_url STRING NOT NULL,          -- The BLS source directory URL that was ingested
  started_at TIMESTAMP,                -- Timestamp when the ingestion run began
  completed_at TIMESTAMP,              -- Timestamp when the ingestion run finished
  run_status STRING,                   -- Overall status: SUCCEEDED or FAILED
  discovered BIGINT,                    -- Total number of files discovered on the BLS server
  downloaded BIGINT,                    -- Number of files newly downloaded in this run
  changed BIGINT,                       -- Number of files whose content changed since the last run
  unchanged BIGINT,                     -- Number of files that were unchanged and skipped
  removed BIGINT,                       -- Number of files that were removed upstream since the last run
  failed BIGINT,                        -- Number of files that failed to download
  error_message STRING                 -- Summary error message if any files failed
) USING DELTA
""")

# Create the raw directory inside the volume if it does not already exist
dbutils.fs.mkdirs(RAW_ROOT)

# COMMAND ----------

# BLS-compliant HTTP session
# This cell sets up a requests-based HTTP session with retries, rate limiting, and a BLS-compliant User-Agent.

# Import hashlib for computing SHA-256 checksums of downloaded files
import hashlib
# Import html for unescaping HTML entities in directory listing links
import html
# Import os for filesystem operations (creating directories, moving/removing temp files)
import os
# Import posixpath for URL-safe path manipulation (forward-slash separators)
import posixpath
# Import time for rate-limiting delays between requests
import time
# Import uuid for generating unique run IDs and temporary file names
import uuid
# Import datetime and timezone for UTC timestamp generation
from datetime import datetime, timezone
# Import HTMLParser for parsing BLS directory listing HTML pages
from html.parser import HTMLParser
# Import unquote for URL-decoding, urljoin for resolving relative URLs, urlparse for URL decomposition
from urllib.parse import unquote, urljoin, urlparse

# Import the requests library for making HTTP calls to the BLS server
import requests
# Import HTTPAdapter to mount custom retry policies on the session
from requests.adapters import HTTPAdapter
# Import Retry from urllib3 to configure automatic retry behavior
from urllib3.util.retry import Retry

# Generate a unique UUID string to identify this ingestion run across all tables and logs
RUN_ID = str(uuid.uuid4())
# Record the UTC timestamp when this run started, for the run log table
STARTED_AT = datetime.now(timezone.utc)
# Construct the User-Agent string that identifies this script to BLS, including the contact email
USER_AGENT = f"BLS-Databricks-Raw-Ingestion/1.0 (contact: {CONTACT_EMAIL})"
# Set the minimum delay in seconds between HTTP requests to respect BLS rate limits
REQUEST_DELAY_SECONDS = 1.0
# Set the download chunk size to 8 MB for streaming large files to disk
CHUNK_SIZE = 8 * 1024 * 1024

# Configure the retry policy for transient HTTP errors with exponential backoff
retry_policy = Retry(
    total=5,                                              # Maximum of 5 total retry attempts
    connect=5,                                             # Maximum of 5 connection-level retries
    read=5,                                                # Maximum of 5 read-level retries
    status=5,                                              # Maximum of 5 retries on bad status codes
    backoff_factor=2,                                      # Exponential backoff factor (delays grow as 2^n seconds)
    status_forcelist=[429, 500, 502, 503, 504],           # HTTP status codes that should trigger a retry
    allowed_methods=frozenset(["GET", "HEAD"]),            # Only retry on GET and HEAD methods
    respect_retry_after_header=True,                      # Honor the Retry-After response header if present
)

# Create a persistent HTTP session for connection reuse across multiple requests
bls_session = requests.Session()
# Set default headers on the session: the identifying User-Agent and a wildcard Accept header
bls_session.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
# Create an HTTPAdapter with the retry policy and mount it for HTTPS requests
adapter = HTTPAdapter(max_retries=retry_policy)
bls_session.mount("https://", adapter)
# Mount the same adapter for HTTP requests (in case of non-HTTPS URLs)
bls_session.mount("http://", adapter)


def bls_request(method: str, url: str):
    """Send an identified and rate-limited request to BLS."""
    # Sleep for the configured delay to avoid overwhelming the BLS server with rapid requests
    time.sleep(REQUEST_DELAY_SECONDS)
    # Send the HTTP request using the session, with a 30s connect timeout and 300s read timeout
    response = bls_session.request(
        method=method,
        url=url,
        timeout=(30, 300),
        allow_redirects=True
    )
    # If BLS returns 403 Forbidden, close the response and raise a descriptive PermissionError
    if response.status_code == 403:
        response.close()
        raise PermissionError(
            "BLS returned HTTP 403. Verify the contact-bearing User-Agent, request frequency, "
            f"and network egress status. User-Agent={USER_AGENT!r}; URL={url}"
        )
    # Raise an HTTPError for any other non-2xx status codes
    response.raise_for_status()
    # Return the successful response object for the caller to consume
    return response

# Print the configured User-Agent to confirm BLS identification is set up correctly
print(f"Configured User-Agent: {USER_AGENT}")

# COMMAND ----------

# Validate access before ingestion
# This cell sends a test GET request to the BLS source URL to confirm connectivity and permissions.

# Open a context-managed GET request to the BLS source directory URL using the configured session
with bls_request("GET", SOURCE_URL) as response:
    # Print the HTTP status code returned by BLS to confirm successful access (expect 200)
    print(f"BLS access test: HTTP {response.status_code}")
    # Print the Content-Type header to verify the response is an HTML directory listing
    print(f"Content-Type: {response.headers.get('Content-Type')}")

# COMMAND ----------

# Dynamic recursive discovery
# This cell defines an HTML link parser and a recursive directory crawler to discover all files under the BLS source URL.

# Define a custom HTMLParser subclass that extracts all href attributes from <a> tags in directory listings
class LinkParser(HTMLParser):
    def __init__(self):
        # Call the parent HTMLParser constructor to initialize internal state
        super().__init__()
        # Initialize an empty list to collect discovered href values
        self.links = []

    def handle_starttag(self, tag, attrs):
        # Only process <a> (anchor) tags, which contain links to files and subdirectories
        if tag.lower() != "a":
            return
        # Iterate over all attributes of the anchor tag to find the href
        for name, value in attrs:
            # If this attribute is an href and has a non-empty value, add it to the list
            if name.lower() == "href" and value:
                # Unescape HTML entities (e.g. &amp; -> &) and strip whitespace before storing
                self.links.append(html.unescape(value).strip())


def canonical_directory_url(url: str) -> str:
    # Normalize a directory URL by stripping trailing slashes and appending exactly one trailing slash
    return url.rstrip("/") + "/"


def is_within_source_tree(candidate_url: str, root_url: str) -> bool:
    # Parse the candidate URL into its components (scheme, netloc, path, etc.)
    candidate = urlparse(candidate_url)
    # Parse the root URL into its components for comparison
    root = urlparse(root_url)
    # Return True only if the candidate shares the same scheme, netloc, and its path starts with the root path
    return (
        candidate.scheme == root.scheme
        and candidate.netloc == root.netloc
        and candidate.path.startswith(root.path)
    )


def relative_path_from_url(file_url: str, root_url: str) -> str:
    # Extract and URL-decode the path component of the file URL
    file_path = unquote(urlparse(file_url).path)
    # Extract and URL-decode the path component of the root URL
    root_path = unquote(urlparse(root_url).path)
    # Compute the relative path from root to file using posixpath for forward-slash semantics
    relative_path = posixpath.normpath(posixpath.relpath(file_path, root_path))
    # Reject unsafe paths that escape the root directory (current dir, parent traversal, or absolute paths)
    if relative_path == "." or relative_path.startswith("../") or relative_path.startswith("/"):
        raise ValueError(f"Unsafe source path: {file_url}")
    # Return the validated relative path string
    return relative_path


def discover_bls_files(root_url: str) -> list:
    # Normalize the root URL to ensure it ends with a trailing slash
    root_url = canonical_directory_url(root_url)
    # Print a blank line for visual separation in the notebook output
    print()
    # Initialize the pending queue with the root directory URL to crawl first
    pending = [root_url]
    # Initialize a set to track already-visited directory URLs and prevent infinite loops
    visited = set()
    # Initialize a dictionary to store discovered files keyed by their relative path (deduplicates)
    discovered = {}

    # Process directories in the pending queue until all have been crawled
    while pending:
        # Pop the next directory URL to crawl from the pending queue
        directory_url = pending.pop()
        # Skip this directory if it has already been visited (prevents revisiting)
        if directory_url in visited:
            continue
        # Mark this directory as visited so it won't be crawled again
        visited.add(directory_url)
        # Print the current directory being discovered for progress visibility
        print(f"Discovering: {directory_url}")

        # Fetch the HTML content of this directory listing page using the rate-limited session
        with bls_request("GET", directory_url) as response:
            # Get the Content-Type header, defaulting to an empty string if not present
            content_type = response.headers.get("Content-Type", "")
            # Verify the response is HTML; raise an error if it is not a directory listing
            if "text/html" not in content_type.lower():
                raise ValueError(f"Expected an HTML directory listing at {directory_url}; received {content_type!r}")
            # Create a LinkParser instance to extract all links from the HTML
            parser = LinkParser()
            # Feed the HTML response body to the parser to populate parser.links
            parser.feed(response.text)

        # Iterate over every href extracted from the directory listing
        for href in parser.links:
            # Skip empty links, self/parent references, and query/fragment-only links
            if not href or href in ("../", "./", "/") or href.startswith(("?", "#")):
                continue
            # Resolve the href against the current directory URL to get an absolute URL
            candidate_url = urljoin(directory_url, href)
            # Skip links that fall outside the source root tree (different host or path prefix)
            if not is_within_source_tree(candidate_url, root_url):
                continue
            # Parse the candidate URL to check its scheme
            parsed = urlparse(candidate_url)
            # Only follow HTTP and HTTPS links; skip mailto, javascript, etc.
            if parsed.scheme not in ("http", "https"):
                continue
            # If the path ends with a slash, this is a subdirectory; add it to the pending queue
            if parsed.path.endswith("/"):
                # Canonicalize the subdirectory URL to ensure a trailing slash
                child = canonical_directory_url(candidate_url)
                # Only add to pending if we haven't already visited this subdirectory
                if child not in visited:
                    pending.append(child)
                # Continue to the next link without treating this as a file
                continue
            # Compute the relative path of this file from the source root
            relative_path = relative_path_from_url(candidate_url, root_url)
            # Store the file in the discovered dictionary, keyed by relative path for deduplication
            discovered[relative_path] = {"source_url": candidate_url, "relative_path": relative_path}

    # Return the discovered files sorted alphabetically by relative path for deterministic ordering
    return sorted(discovered.values(), key=lambda item: item["relative_path"])


# Discover all files beneath the configured BLS source URL by crawling directory listings recursively
source_files = discover_bls_files(SOURCE_URL)
# If no files were found, raise an error to prevent an empty inventory from triggering mass deletion logic
if not source_files:
    raise RuntimeError("No files were discovered. Stopping to prevent an empty inventory from being treated as deletion.")
# Print the total number of files discovered for visibility in the notebook output
print(f"Discovered {len(source_files)} files")

# COMMAND ----------

# Read current manifest
# This cell loads the existing manifest from the Delta table into a Python dict for fast lookups during processing.

# Import PySpark SQL functions for filtering the manifest table
from pyspark.sql import functions as F

# Query the manifest Delta table, filter to rows matching the current source URL, and collect all rows into the driver
# Build a dictionary mapping each file's relative_path to its full row as a recursive dict
manifest_rows = {
    row["relative_path"]: row.asDict(recursive=True)
    for row in spark.table(MANIFEST_TABLE_NAME).filter(F.col("source_root") == SOURCE_URL).collect()
}
# Print the number of manifest records loaded for visibility in the notebook output
print(f"Loaded {len(manifest_rows)} manifest records")
print(manifest_rows)

# COMMAND ----------

# Metadata and download helpers
# This cell defines functions for fetching source metadata, comparing it to the manifest, and downloading files.

# Parse the Content-Length header from an HTTP response into an integer, returning None if absent or invalid
def parse_content_length(headers):
    # Retrieve the Content-Length header value from the response headers
    value = headers.get("Content-Length")
    try:
        # Attempt to convert the header value to an integer if it exists
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        # Return None if the value cannot be parsed as an integer
        return None


# Send a HEAD request to the BLS server to fetch metadata (ETag, Last-Modified, Content-Length) without downloading the body
def get_source_metadata(url: str) -> dict:
    try:
        # Use a context-managed HEAD request to fetch only the response headers
        with bls_request("HEAD", url) as response:
            # Return a dictionary with the three metadata fields used for change detection
            return {
                "etag": response.headers.get("ETag"),                # Entity tag for version identification
                "last_modified": response.headers.get("Last-Modified"),  # Last modification timestamp from server
                "content_length": parse_content_length(response.headers),   # Content size in bytes
            }
    except requests.HTTPError as exc:
        # If the server does not support HEAD (returns 405 or 501), return null metadata instead of failing
        status = exc.response.status_code if exc.response is not None else None
        if status not in (405, 501):
            raise
        # Return all-None metadata when HEAD is not supported by the server
        return {"etag": None, "last_modified": None, "content_length": None}


# Compare previous manifest metadata with current source metadata to determine if a re-download can be skipped
def metadata_matches(previous, current) -> bool:
    # If there is no previous record, or the previous ingestion was not complete or not active, force a re-download
    if not previous or previous.get("ingestion_status") != "COMPLETE" or previous.get("source_status") != "ACTIVE":
        return False
    # Initialize a list to collect boolean comparison results for each available metadata field
    comparisons = []
    # If the current ETag is present, compare it against the previously stored ETag
    if current.get("etag") is not None:
        comparisons.append(current["etag"] == previous.get("source_etag"))
    # If the current Last-Modified is present, compare it against the previously stored value
    if current.get("last_modified") is not None:
        comparisons.append(current["last_modified"] == previous.get("source_last_modified"))
    # If the current Content-Length is present, compare it against the previously stored value
    if current.get("content_length") is not None:
        comparisons.append(current["content_length"] == previous.get("source_content_length"))
    # Return True only if at least one comparison was made and all comparisons matched
    return bool(comparisons) and all(comparisons)


# Construct the full destination path in the Unity Catalog volume for a given relative path
def destination_path(relative_path: str) -> str:
    # Normalize the relative path to remove any redundant separators or references
    safe_path = posixpath.normpath(relative_path)
    # Reject paths that try to escape the raw root directory via parent traversal or absolute paths
    if safe_path.startswith("../") or safe_path.startswith("/"):
        raise ValueError(f"Unsafe destination path: {relative_path}")
    # Return the full volume path by concatenating the raw root with the safe relative path
    return f"{RAW_ROOT}/{safe_path}"


# Download a file from the BLS server to a temporary file, computing its SHA-256 hash during download
def download_to_temporary_file(source_url: str, final_path: str) -> dict:
    # Create the parent directory of the final path so the temp file can be placed alongside it
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    # Generate a unique temporary file path by appending the run ID and a random hex suffix to the final path
    temporary_path = f"{final_path}.partial-{RUN_ID}-{uuid.uuid4().hex}"
    # Initialize a SHA-256 hash object to compute the checksum as data is streamed
    digest = hashlib.sha256()
    # Initialize a counter to track the total number of bytes downloaded
    total_bytes = 0
    try:
        # Open a context-managed GET request to stream the file content from the BLS server
        with bls_request("GET", source_url) as response:
            # Extract metadata from the response headers for the manifest record
            metadata = {
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "content_length": parse_content_length(response.headers),
            }
            # Open the temporary file for writing in binary mode
            with open(temporary_path, "wb") as output:
                # Iterate over the response content in chunks of CHUNK_SIZE bytes
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    # Skip empty chunks that can occur with chunked transfer encoding
                    if chunk:
                        # Write the chunk to the temporary file on disk
                        output.write(chunk)
                        # Update the SHA-256 hash with the current chunk's bytes
                        digest.update(chunk)
                        # Accumulate the byte count for the file size
                        total_bytes += len(chunk)
        # Optional integrity check: compare actual bytes downloaded to Content-Length if provided (currently commented out)
        #if metadata["content_length"] is not None and total_bytes != metadata["content_length"]:
        #    raise IOError(f"Incomplete download of {source_url}: expected {metadata['content_length']} bytes, got {total_bytes}")
        # Return a dictionary with the temp path, computed hash, file size, and source metadata
        return {
            "temporary_path": temporary_path,
            "sha256": digest.hexdigest(),
            "file_size": total_bytes,
            **metadata,
        }
    except Exception:
        # If any error occurs during download, clean up the temporary file if it exists
        if os.path.exists(temporary_path):
            os.remove(temporary_path)
        # Re-raise the exception so the caller can handle the failure
        raise

# COMMAND ----------

# Delta manifest merge helper
# This cell defines the Spark schema for manifest records and a merge function to upsert them into the Delta table.

# Import DeltaTable to perform conditional upserts (merge) on the Delta manifest table
from delta.tables import DeltaTable
# Import Spark SQL types needed to define the manifest record schema
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

# Define the explicit schema for manifest update records, matching the Delta table column structure
manifest_schema = StructType([
    StructField("source_root", StringType(), False),          # Root BLS URL (non-nullable)
    StructField("source_url", StringType(), False),           # Full source file URL (non-nullable)
    StructField("relative_path", StringType(), False),        # Path relative to source root (non-nullable)
    StructField("destination_path", StringType(), False),     # Volume path where file is stored (non-nullable)
    StructField("source_etag", StringType(), True),           # ETag from BLS server (nullable)
    StructField("source_last_modified", StringType(), True),  # Last-Modified from BLS server (nullable)
    StructField("source_content_length", LongType(), True),  # Content-Length from BLS server (nullable)
    StructField("sha256", StringType(), True),                # SHA-256 hash of file content (nullable)
    StructField("file_size", LongType(), True),               # Actual downloaded file size in bytes (nullable)
    StructField("source_status", StringType(), True),        # ACTIVE / REMOVED / UNKNOWN status (nullable)
    StructField("ingestion_status", StringType(), True),      # COMPLETE / FAILED status (nullable)
    StructField("first_seen_at", TimestampType(), True),      # When the file was first discovered (nullable)
    StructField("last_seen_at", TimestampType(), True),       # When the file was last seen (nullable)
    StructField("ingested_at", TimestampType(), True),       # When the file was last successfully downloaded (nullable)
    StructField("removed_at", TimestampType(), True),        # When the file was marked removed upstream (nullable)
    StructField("last_run_id", StringType(), True),           # UUID of the run that last processed this file (nullable)
    StructField("error_message", StringType(), True),         # Error message if processing failed (nullable)
])


# Merge a batch of manifest update records into the Delta manifest table using upsert logic
def merge_manifest(records):
    # If the records list is empty, skip the merge to avoid creating an empty DataFrame
    if not records:
        return
    # Create a Spark DataFrame from the Python list of dict records using the explicit schema
    updates = spark.createDataFrame(records, schema=manifest_schema)
    # Get a DeltaTable handle for the manifest table to perform a merge operation
    target = DeltaTable.forName(spark, MANIFEST_TABLE_NAME)
    # Execute a merge (upsert): match on source_root + relative_path, update if matched, insert if not matched
    (
        target.alias("target")
        .merge(
            updates.alias("source"),
            "target.source_root = source.source_root AND target.relative_path = source.relative_path",
        )
        .whenMatchedUpdateAll()      # If a matching row exists, update all columns with the new values
        .whenNotMatchedInsertAll()   # If no matching row exists, insert the new record
        .execute()                   # Execute the merge operation against the Delta table
    )

# COMMAND ----------

# Process added, changed, and unchanged files
# This cell iterates over all discovered files, compares them to the manifest, downloads new/changed files,
# and builds manifest update records for each file.

# Initialize counters to track the outcome of each file in this ingestion run
counters = {"discovered": len(source_files), "downloaded": 0, "changed": 0, "unchanged": 0, "removed": 0, "failed": 0}
# Initialize an empty list to collect manifest update records for batch merging
manifest_updates = []
# Initialize a set to track all relative paths discovered in this run (used later to detect removals)
current_paths = set()
# Record the current UTC timestamp for use as the "now" value in manifest records
now = datetime.now(timezone.utc)

# Iterate over each discovered file with a 1-based index for progress display
for index, item in enumerate(source_files, start=1):
    # Extract the full source URL for this file from the discovery result
    source_url = item["source_url"]
    # Extract the relative path (used as the manifest key) from the discovery result
    relative_path = item["relative_path"]
    # Add this path to the set of currently discovered paths for removal detection
    current_paths.add(relative_path)
    # Compute the destination path in the Unity Catalog volume for this file
    final_path = destination_path(relative_path)
    # Look up the previous manifest record for this file, if any exists
    previous = manifest_rows.get(relative_path)
    # Print progress showing the current file index, total count, and relative path
    print(f"[{index}/{len(source_files)}] {relative_path}")

    try:
        # Fetch current metadata (ETag, Last-Modified, Content-Length) from the BLS server via HEAD request
        metadata = get_source_metadata(source_url)
        # If metadata matches the previous record AND the file already exists on disk, skip the download
        if metadata_matches(previous, metadata) and os.path.exists(final_path):
            # Increment the unchanged counter since no download was needed
            counters["unchanged"] += 1
            # Append a manifest update preserving the previous record with refreshed timestamps
            manifest_updates.append({
                **previous,                           # Spread all previous fields as the base
                "source_url": source_url,            # Update the source URL in case it changed
                "destination_path": final_path,     # Update the destination path in case it changed
                "source_status": "ACTIVE",           # Confirm the file is still active on the BLS server
                "ingestion_status": "COMPLETE",      # Mark ingestion as complete (unchanged)
                "last_seen_at": now,                 # Update the last-seen timestamp to this run
                "removed_at": None,                  # Clear any previous removal timestamp
                "last_run_id": RUN_ID,               # Record the current run ID
                "error_message": None,               # Clear any previous error message
            })
            # Skip to the next file since no download was necessary
            continue

        # Download the file to a temporary location, computing SHA-256 during download
        downloaded = download_to_temporary_file(source_url, final_path)
        # Retrieve the previous SHA-256 hash if a previous record exists, otherwise None
        previous_hash = previous.get("sha256") if previous else None
        # Determine whether content has changed: no previous hash, hash mismatch, or file missing on disk
        content_changed = previous_hash is None or downloaded["sha256"] != previous_hash or not os.path.exists(final_path)

        if content_changed:
            # Atomically replace the old file (if any) with the newly downloaded temporary file
            os.replace(downloaded["temporary_path"], final_path)
            # Increment the downloaded counter for a newly downloaded file
            counters["downloaded"] += 1
            # If there was a previous hash, this is a changed file (not a new file); increment changed counter
            if previous_hash is not None:
                counters["changed"] += 1
            # Set the ingested timestamp to now since a new download occurred
            ingested_at = now
        else:
            # Content is unchanged despite metadata mismatch; remove the temporary file
            os.remove(downloaded["temporary_path"])
            # Increment the unchanged counter
            counters["unchanged"] += 1
            # Preserve the previous ingested_at timestamp since no new download occurred
            ingested_at = previous.get("ingested_at")

        # Append a manifest update record with the latest metadata and status for this file
        manifest_updates.append({
            "source_root": SOURCE_URL,                                                                       # Root BLS directory URL
            "source_url": source_url,                                                                       # Full source file URL
            "relative_path": relative_path,                                                                 # Path relative to source root
            "destination_path": final_path,                                                                # Volume storage path
            "source_etag": downloaded["etag"] or metadata.get("etag"),                                     # Prefer download ETag, fall back to HEAD metadata
            "source_last_modified": downloaded["last_modified"] or metadata.get("last_modified"),           # Prefer download Last-Modified, fall back to HEAD
            "source_content_length": downloaded["content_length"] if downloaded["content_length"] is not None else metadata.get("content_length"),  # Prefer download Content-Length, fall back to HEAD
            "sha256": downloaded["sha256"],                                                                # SHA-256 hash of downloaded content
            "file_size": downloaded["file_size"],                                                          # Actual byte count of downloaded file
            "source_status": "ACTIVE",                                                                     # File is active on the BLS server
            "ingestion_status": "COMPLETE",                                                                # Ingestion completed successfully
            "first_seen_at": previous.get("first_seen_at") if previous else now,                          # Preserve first-seen or set to now for new files
            "last_seen_at": now,                                                                            # Update last-seen to this run
            "ingested_at": ingested_at,                                                                    # Set ingested_at from download or previous
            "removed_at": None,                                                                              # Clear removal timestamp
            "last_run_id": RUN_ID,                                                                           # Record the current run ID
            "error_message": None,                                                                          # Clear any previous error
        })
    except PermissionError:
        # Re-raise PermissionError immediately (HTTP 403 is a fatal access issue that should stop the run)
        raise
    except Exception as exc:
        # Increment the failed counter for any other exception during processing
        counters["failed"] += 1
        # Append a manifest update record marking this file as failed with the previous metadata preserved
        manifest_updates.append({
            "source_root": SOURCE_URL,                                                                       # Root BLS directory URL
            "source_url": source_url,                                                                       # Full source file URL
            "relative_path": relative_path,                                                                 # Path relative to source root
            "destination_path": final_path,                                                                # Volume storage path
            "source_etag": previous.get("source_etag") if previous else None,                              # Preserve previous ETag or None
            "source_last_modified": previous.get("source_last_modified") if previous else None,           # Preserve previous Last-Modified or None
            "source_content_length": previous.get("source_content_length") if previous else None,        # Preserve previous Content-Length or None
            "sha256": previous.get("sha256") if previous else None,                                        # Preserve previous hash or None
            "file_size": previous.get("file_size") if previous else None,                                  # Preserve previous file size or None
            "source_status": "UNKNOWN",                                                                    # Source status is unknown due to failure
            "ingestion_status": "FAILED",                                                                  # Mark ingestion as failed
            "first_seen_at": previous.get("first_seen_at") if previous else now,                          # Preserve first-seen or set to now
            "last_seen_at": now,                                                                            # Update last-seen to this run
            "ingested_at": previous.get("ingested_at") if previous else None,                            # Preserve previous ingested_at or None
            "removed_at": previous.get("removed_at") if previous else None,                               # Preserve previous removed_at or None
            "last_run_id": RUN_ID,                                                                           # Record the current run ID
            "error_message": str(exc)[:4000],                                                               # Store the error message (truncated to 4000 chars)
        })
        # Print a failure message with the relative path and exception details for debugging
        print(f"FAILED: {relative_path}: {exc}")

# Merge all accumulated manifest update records into the Delta manifest table in a single batch operation
merge_manifest(manifest_updates)

# COMMAND ----------

# Mark files removed upstream while preserving landed raw copies
# This cell identifies files that were in the manifest as ACTIVE but were not discovered in this run,
# marking them as REMOVED in the manifest without deleting the raw files from the volume.

# Initialize an empty list to collect manifest update records for removed files
removed_updates = []
# Record the current UTC timestamp as the removal time for files no longer present upstream
removal_time = datetime.now(timezone.utc)
# Iterate over every file in the existing manifest to check if it still exists upstream
for relative_path, previous in manifest_rows.items():
    # If the file was previously ACTIVE but was not discovered in this run, mark it as removed
    if previous.get("source_status") == "ACTIVE" and relative_path not in current_paths:
        # Append a manifest update record preserving the previous data but changing status to REMOVED
        removed_updates.append({
            **previous,                       # Spread all previous fields as the base
            "source_status": "REMOVED",       # Change source status to REMOVED (no longer on BLS server)
            "ingestion_status": "COMPLETE",    # Mark ingestion status as complete (removal was processed)
            "removed_at": removal_time,       # Set the removal timestamp to the current run time
            "last_run_id": RUN_ID,             # Record the current run ID that detected the removal
            "error_message": None,             # Clear any previous error message
        })

# Update the removed counter with the number of files marked as removed
counters["removed"] = len(removed_updates)
# Merge all removal update records into the Delta manifest table in a single batch operation
merge_manifest(removed_updates)
# Print the number of files marked as removed for visibility in the notebook output
print(f"Marked {counters['removed']} files as removed upstream")

# COMMAND ----------

# Record run result and exit
# This cell writes a summary row for the ingestion run to the run log Delta table and exits the notebook.

# Import Spark SQL types needed to define the run log record schema
from pyspark.sql.types import (
    StructType,       # Schema definition for a DataFrame row structure
    StructField,      # Individual column definition within a StructType
    StringType,       # String column type for text fields
    TimestampType,    # Timestamp column type for date/time fields
    IntegerType      # Integer column type for numeric counters
)

# Define the schema for the run log record, matching the run table column structure
run_schema = StructType([
    StructField("run_id", StringType(), False),         # Unique run UUID (non-nullable)
    StructField("source_url", StringType(), False),      # BLS source directory URL (non-nullable)
    StructField("started_at", TimestampType(), False),   # Run start timestamp (non-nullable)
    StructField("completed_at", TimestampType(), True),  # Run completion timestamp (nullable)
    StructField("run_status", StringType(), False),      # SUCCEEDED or FAILED status (non-nullable)
    StructField("discovered", IntegerType(), False),      # Count of files discovered (non-nullable)
    StructField("downloaded", IntegerType(), False),     # Count of files downloaded (non-nullable)
    StructField("changed", IntegerType(), False),        # Count of files changed (non-nullable)
    StructField("unchanged", IntegerType(), False),      # Count of files unchanged (non-nullable)
    StructField("removed", IntegerType(), False),        # Count of files removed upstream (non-nullable)
    StructField("failed", IntegerType(), False),         # Count of files that failed (non-nullable)
    StructField("error_message", StringType(), True),   # Error summary if any files failed (nullable)
])


# Record the UTC timestamp when the ingestion run completed
completed_at = datetime.now(timezone.utc)
# Determine the overall run status: FAILED if any files failed, otherwise SUCCEEDED
run_status = "FAILED" if counters["failed"] else "SUCCEEDED"
# Build a single-element list containing the run summary record as a dictionary
run_record = [{
    "run_id": RUN_ID,                              # Unique identifier for this run
    "source_url": SOURCE_URL,                      # BLS source directory that was ingested
    "started_at": STARTED_AT,                      # Timestamp when the run started (recorded earlier)
    "completed_at": completed_at,                  # Timestamp when the run completed
    "run_status": run_status,                       # Overall SUCCEEDED or FAILED status
    "discovered": counters["discovered"],           "downloaded": counters["downloaded"],           "changed": counters["changed"],              # Count of files whose content changed
    "unchanged": counters["unchanged"],            # Count of files that were unchanged and skipped
    "removed": counters["removed"],                # Count of files marked as removed upstream
    "failed": counters["failed"],                  # Count of files that failed to download
    "error_message": f"{counters['failed']} file(s) failed" if counters["failed"] else None,  # Error summary or None
}]
# Print the run record dictionary for visibility in the notebook output
print(run_record)
# Create a Spark DataFrame from the run record using the explicit schema
df = spark.createDataFrame(
    run_record,
    schema=run_schema
)
# Print the DataFrame contents in a readable tabular format
df.show()
# Write the run record to the run log Delta table in append mode (each run adds a new row)
df.write.mode("append").saveAsTable(RUN_TABLE_NAME)
# Build a summary dictionary combining the run ID, status, user agent, and all counters
summary = {"run_id": RUN_ID, "status": run_status, "user_agent": USER_AGENT, **counters}
# Print the summary dictionary for visibility in the notebook output
print(summary)
# If any files failed, raise a RuntimeError to signal the failure to any calling job or pipeline
if counters["failed"]:
    raise RuntimeError(f"BLS ingestion failed for {counters['failed']} file(s). Run ID: {RUN_ID}")
# Exit the notebook, returning the summary as a JSON string for any calling job or pipeline
dbutils.notebook.exit(str(summary))