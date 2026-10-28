# Rearc Data Quest: Process and Design Decisions

## Overview

I approached this project as a small, production-style data pipeline rather than a one-time analysis. The goal was to preserve the source data, make ingestion safe to rerun, validate the data before analysis, and create Gold tables that directly answer the assignment questions.

The solution has two main parts:

1. Ingest the Bureau of Labor Statistics (BLS) and Data USA files into a Databricks Unity Catalog Volume.
2. Process the landed data through Bronze, Silver, and Gold layers using a Spark Declarative Pipeline.

## Architecture

### Raw ingestion

I kept source acquisition separate from transformation. Python handles the external HTTP requests and lands the files in a Unity Catalog Volume. The Spark pipeline reads those files and performs the validation, cleanup, and modeling.

This separation preserves the original source files and makes it easier to determine whether a failure occurred during download or transformation.

### BLS ingestion

The BLS ingestion process dynamically discovers files from the configured BLS directory instead of relying on a hardcoded file list. Requests include a contact-bearing User-Agent because anonymous programmatic requests can receive HTTP 403 responses.

Two Delta control tables track ingestion:

- `bls_ingestion_manifest` stores the current state of each source file.
- `bls_ingestion_runs` records each run's outcome and file counts.

The process first checks HTTP metadata Last-Modified. If the metadata matches the previous successful ingestion and the landed file still exists, the file is skipped.

When the metadata cannot confirm that a file is unchanged, the file is downloaded to a temporary location and compared using SHA-256. New files are added, changed files replace the previous raw copy, and unchanged temporary files are discarded.

Files that disappear from the source directory are marked `REMOVED` in the manifest, but their landed copies are retained for traceability and recovery.

Both `pr.data.0.Current` and `pr.data.1.AllData` are landed because the assignment requires the complete BLS source directory. The analytical pipeline uses `pr.data.1.AllData` because it contains the complete history. Combining both files would duplicate current-year observations.

### Bronze

Bronze represents the landed source with minimal changes.

Separate BLS Bronze tables store:

- historical observations;
- series definitions; and
- sector, class, measure, duration, seasonal, period, and footnote mappings.

The BLS files are tab-delimited text files with headers. Source lineage is retained using `_metadata.file_path`, which is supported for files read through Unity Catalog.

The Data USA population response is also landed before transformation. Fields that are not suitable as standard Delta column names are normalized, such as changing `Nation ID` to `nation_id`.

### Silver

Silver makes the source data reliable and consistent for analysis. The transformations include:

- trimming and standardizing fields;
- casting years and values to appropriate types;
- validating required keys and numeric values;
- validating supported BLS periods;
- deduplicating BLS observations by `(series_id, year, period)`;
- normalizing the population data; and
- enriching BLS series IDs with readable descriptions.

The population data is unique by nation and year for this use case.

For the BLS series description, `pr.series` is joined to the sector, class, measure, duration, and seasonal mapping data. This produces a readable `series_label`, allowing someone unfamiliar with BLS series codes to understand what each series measures.

Spark Declarative Pipeline expectations enforce basic quality requirements, including non-null keys, valid numeric values, and supported periods.

### Gold

Gold contains outputs designed specifically for the three assignment questions.

#### 1. US population statistics

The population data is filtered to the United States for 2013 through 2018 inclusive.

The output calculates the mean and both population and sample standard deviations. Both standard deviations are included because the question does not specify which convention to use.

#### 2. Best year for each BLS series

The calculation filters to `Q01` through `Q04`. It excludes `Q05` because BLS defines it as an annual average, not a fifth quarter.

The transformation:

1. Groups observations by `series_id` and year.
2. Sums the values.
3. Ranks the years within each series by descending annual total.
4. Uses the earliest year as a deterministic tie-breaker.
5. Adds the human-readable `series_label`.

#### 3. PRS30006032 joined with population

The final output filters the BLS observations to:

- `series_id = PRS30006032`
- `period = Q01`

It then left joins the annual US population data by year.

The left join is intentional because the question asks for population "where available." A BLS observation remains in the result even when the population dataset does not contain that year.

## PySpark and SQL

PySpark is the primary implementation because the pipeline is Python-based and the DataFrame API supports reusable, testable transformations.

Equivalent Spark SQL is included for the three Gold analytical questions because SQL is especially readable for aggregation, ranking, filtering, and joins.

## Safe Reruns and Idempotency

The BLS ingestion process handles reruns by:

- discovering new files dynamically;
- skipping files confirmed to be unchanged;
- comparing uncertain files using SHA-256;
- replacing changed files only after a successful download;
- recording removed files without immediately deleting them;
- preserving the previous valid file when a download fails; and
- logging each ingestion run.

This prevents the pipeline from blindly downloading and rewriting every source file during each run.

Silver uses stable business keys and deterministic transformations so that processing the same raw inputs produces the same modeled results.

## Trade-offs

For this assignment, I prioritized a simple and rerunnable solution over production-level complexity. For a real client implementation, I would improve the following areas.

### Schema drift

The current schemas are based on the BLS and Data USA source structures. In production, I would version the expected schemas, quarantine incompatible files or records, and alert when the source changes unexpectedly.

### Data volume and performance

The BLS PR dataset is small enough for straightforward Spark processing. For larger datasets, I would evaluate incremental processing, partitioning, clustering, and streaming based on the source's update behavior.

I would not automatically use Auto Loader for `pr.data.1.AllData` because it is a mutable snapshot. Historical values can be revised, so the ingestion strategy must detect updates rather than assume that the source is append-only.

### Cost

The assignment's data volume does not justify complex optimization. In production, I would monitor runtime, input size, shuffle volume, compute utilization, and table size before making optimization decisions.

Compute size, scheduling frequency, and incremental processing would be based on the client's workload and service-level requirements.

### Access control

Unity Catalog permissions would follow least-privilege principles.

The ingestion identity would have write access to the raw Volume and control tables. Pipeline identities would receive only the permissions required to read sources and write target tables. Analysts would normally receive read access to approved Gold tables rather than the raw files or operational control tables.

### Monitoring

The manifest and run tables provide basic operational visibility. A production implementation would add alerts for:

- failed ingestion runs;
- missing source files;
- unexpected file-size changes;
- schema changes;
- data-quality expectation failures;
- stale data; and
- pipeline SLA violations.

Monitoring could be implemented through Databricks Jobs and Lakeflow notifications, an operational dashboard, or the client's observability platform.

### Removed source files

I chose to mark removed BLS files in the manifest while preserving the landed copies. This favors traceability and recovery over maintaining a destructive mirror of the source directory.

A production environment would require an agreed retention policy for archiving or deleting files that are no longer available upstream.

## Retrospective

The analytical questions were straightforward. The hardest part was handling the external sources correctly and making ingestion safe to rerun.

The main challenges were:

- adding a contact-bearing User-Agent to avoid BLS HTTP 403 errors;
- discovering BLS files dynamically;
- distinguishing new, changed, unchanged, and removed files;
- recognizing the overlap between `pr.data.0.Current` and `pr.data.1.AllData`;
- joining the BLS mapping files to create readable series labels;
- recognizing that `Q05` is an annual average rather than a fifth quarter; and
- normalizing Data USA fields such as `Nation ID` for Delta compatibility.

Overall, I prioritized correctness, rerunnability, traceability, and simple deterministic transformations. For the size of this assignment, those choices provided more value than adding infrastructure that would increase complexity without improving the analytical results.

## AI Assistance

I used Microsoft Copilot to help scaffold the ingestion and Spark Declarative Pipeline code and refine the documentation.  I reviewed and adapted the output for the project's requirements.