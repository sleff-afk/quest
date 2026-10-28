# Rearc Data Quest: Process and Design Decisions

## Overview

I built the project as a rerunnable Databricks data pipeline rather than a one-time analysis. It ingests Bureau of Labor Statistics (BLS) files and Data USA population data into a Unity Catalog Volume, then processes them through Bronze, Silver, and Gold layers using a Spark Declarative Pipeline.

## Architecture

### Raw ingestion

Source acquisition is separate from transformation. Python retrieves the external files and stores them in a Unity Catalog Volume, while the pipeline validates and models the landed data. This preserves the original files and makes failures easier to isolate.

### BLS ingestion

The ingestion process discovers files dynamically from the BLS directory and uses a contact-bearing User-Agent for programmatic requests.

Two Delta control tables track ingestion:

- `bls_ingestion_manifest` stores the state of each source file.
- `bls_ingestion_runs` records each run's outcome and counts.

HTTP metadata such as ETag, Last-Modified, and Content-Length is used to skip unchanged files. When that metadata is insufficient, the file is downloaded temporarily and compared using SHA-256. Changed files replace the existing raw copy atomically, new files are added, and files removed upstream are marked `REMOVED` without deleting the landed copy.

Both `pr.data.0.Current` and `pr.data.1.AllData` are landed, but analysis uses `pr.data.1.AllData` because it contains the complete history. Combining both would duplicate current-year observations.

### Bronze

Bronze represents the landed data with minimal changes. Separate BLS tables store observations, series definitions, and mapping data. The source files are tab-delimited, and lineage is retained with `_metadata.file_path`.

Data USA JSON fields are normalized where needed for Delta compatibility, such as changing `Nation ID` to `nation_id`.

### Silver

Silver makes the data reliable for analysis by:

- trimming and standardizing fields;
- casting years and values to appropriate types;
- validating required keys, periods, and numeric values;
- deduplicating BLS observations by `(series_id, year, period)`;
- normalizing population data; and
- joining BLS mapping tables to create a readable `series_label`.

The population data is unique by nation and year. Spark Declarative Pipeline expectations enforce core quality rules.

### Gold

Gold tables directly answer the three assignment questions.

#### Population statistics

The population data is filtered to the United States for 2013 through 2018. The output includes the mean and both population and sample standard deviations because the question does not specify which convention to use.

#### Best year by BLS series

The calculation excludes `Q05` because it is an annual average, not a fifth quarter. It sums `Q01` through `Q04` by `series_id` and year, retains `quarter_count`, ranks each series by annual total, uses the earliest year to break ties, and includes the readable series label.

I retained incomplete years rather than silently removing them. The `quarter_count` column makes completeness visible.

#### PRS30006032 joined to population

The final query filters to `series_id = PRS30006032` and `period = Q01`, then left joins US population by year. A left join preserves every BLS observation and returns a null population when that year is unavailable.

## PySpark and SQL

PySpark is the primary implementation because the pipeline is Python-based and the DataFrame API supports reusable, testable transformations. Equivalent SQL is included for the three analyses because SQL is especially clear for aggregation, ranking, and joins.

Only one implementation materializes each Gold output, avoiding duplicate tables and maintenance.

## Safe reruns

BLS ingestion handles reruns by discovering files, skipping unchanged content, replacing changed files safely, recording removed files, preserving valid files after failed downloads, and logging each run.

Stable natural keys and deterministic Silver transformations ensure that the same raw inputs produce the same modeled results.

## Production considerations

For a production client, I would add:

- versioned schema checks and quarantine handling;
- alerts for failed runs, stale data, missing files, size changes, and expectation failures;
- incremental processing for larger datasets where source update behavior supports it;
- compute and scheduling based on actual volume and SLA requirements;
- Unity Catalog least-privilege access for ingestion, pipeline, and analyst identities; and
- a retention policy for raw files removed upstream.

I would not automatically use Auto Loader for `pr.data.1.AllData` because it is a mutable snapshot and historical values may be revised.

## Retrospective

The analytical queries were straightforward. The harder part was handling the sources correctly.

Key issues included the BLS User-Agent requirement, dynamic file discovery, change detection, overlap between the current and historical data files, metadata joins for readable labels, and recognizing that `Q05` is an annual average.

Separating ingestion metadata from analytical data also proved important. The manifest and run tables support auditability and safe reruns without mixing operational state into Bronze business data.

Overall, I prioritized correctness, rerunnability, transparency, and simple deterministic transformations appropriate for the size of the assignment.

## AI Assistance

I used Microsoft Copilot to help scaffold code, review SQL and PySpark approaches, develop tests, and refine documentation. I reviewed and adapted the output against the assignment requirements, source documentation, and observed Databricks behavior. I am responsible for the final implementation and design decisions.
