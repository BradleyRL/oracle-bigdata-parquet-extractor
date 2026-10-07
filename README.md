# Oracle Big Data Parquet Extractor

A high-performance Python tool designed for extracting massive datasets from Oracle database tables using predefined `.sql` script files and streaming the output directly to **Parquet** files with minimal RAM utilization.

---

## Key Features

- **Constant Memory Footprint**: Uses cursor batch streaming (`pyarrow.ParquetWriter`) to process multi-gigabyte or terabyte tables without memory overflow.
- **Predefined `.sql` File Execution**: Reads and executes any standard `.sql` script from disk.
- **Oracle Network Tuning**: Custom cursor `arraysize` and `prefetchrows` (default: 50,000) for maximum throughput over network connections.
- **Thin Mode (Default)**: Uses `python-oracledb` in Pure Python Thin mode — no Oracle Client / Instant Client installation needed for modern Oracle DBs.
- **Multi-Worker Parallel Extraction**: Optional parallel extraction mode using `ORA_HASH(ROWID, N)` to distribute table scans across multiple CPU cores/workers.

---

## Installation

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. (Optional) Set up database credentials in a `.env` file:
   ```bash
   cp .env.example .env
   ```
   Edit `.env` with your Oracle connection parameters:
   ```env
   ORACLE_USER=my_user
   ORACLE_PASSWORD=my_password
   ORACLE_DSN=dbhost.example.com:1521/ORCLPDB1
   ```

---

## Quick Start

### 1. Basic Single-Worker Extraction

Execute a predefined query from `sample_query.sql` and save the result to `output.parquet`:

```bash
python extract_oracle_data.py \
  --sql-file sample_query.sql \
  --output data/output.parquet
```

### 2. High-Volume Parallel Extraction (Multi-Worker)

Split extraction across 4 parallel workers using `ORA_HASH(ROWID)` partitioning for faster reads on large tables:

```bash
python extract_oracle_data.py \
  --sql-file sample_query.sql \
  --output data/output \
  --workers 4
```

This will produce partition files:
- `data/output_part_0.parquet`
- `data/output_part_1.parquet`
- `data/output_part_2.parquet`
- `data/output_part_3.parquet`

---

## CLI Options

| Argument | Short | Description | Default |
| :--- | :--- | :--- | :--- |
| `--sql-file` | `-s` | **Required.** Path to `.sql` file containing the SELECT statement. | — |
| `--output` | `-o` | **Required.** Output Parquet filename or directory prefix. | — |
| `--user` | `-u` | Oracle Username (or `ORACLE_USER` env var). | `None` |
| `--password` | `-p` | Oracle Password (or `ORACLE_PASSWORD` env var). | `None` |
| `--dsn` | `-d` | Oracle Connection String / TNS (or `ORACLE_DSN` env var). | `None` |
| `--chunk-size` | — | Rows per batch written per Parquet chunk. | `50000` |
| `--arraysize` | — | Oracle Cursor fetch array size for network buffer tuning. | `50000` |
| `--workers` | `-w` | Number of parallel worker processes. | `1` |
| `--compression` | — | Parquet compression codec (`SNAPPY`, `GZIP`, `ZSTD`, `UNCOMPRESSED`). | `SNAPPY` |
| `--thick-mode` | — | Enable Oracle Thick mode (requires `ORACLE_LIB_DIR` env var). | `False` |

---

## Performance Tuning Tips

1. **`arraysize` & `prefetchrows`**:
   - The script sets `arraysize = 50000` by default. For wide tables (100+ columns), reduce this to `10000`–`20000` to balance network memory buffers. For narrow tables, keep `50000`–`100000`.
2. **Avoid `ORDER BY` in Oracle Query**:
   - Unless ordering is strictly required, remove `ORDER BY` clauses from your `.sql` file. `ORDER BY` forces Oracle to perform full sorts on disk/temp tables before emitting the first row.
3. **Multi-Worker Scaling**:
   - Parallel extraction (`--workers 4` or higher) works best when reading directly from disk tables. If your predefined `.sql` script includes complex aggregations or `JOIN`s, test `--workers 1` first.
