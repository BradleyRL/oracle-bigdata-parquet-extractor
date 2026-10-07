#!/usr/bin/env python3
"""
High-Performance Oracle Big Data Extractor to Parquet
=====================================================

Streams large datasets from an Oracle table/query into Parquet format with minimal RAM usage.
Supports:
  - Reading predefined .sql query files
  - python-oracledb in Thin mode (default) or Thick mode
  - High-throughput fetch batching (arraysize / prefetchrows tuning)
  - Streaming output directly to Parquet using PyArrow
  - Optional multi-worker parallel extraction via ORA_HASH(ROWID)
"""

import os
import sys
import time
import argparse
import datetime
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Stream big data from an Oracle database table using a predefined SQL query file to Parquet format."
    )
    
    # Required / Core options
    parser.add_argument(
        "--sql-file", "-s", required=True, type=str,
        help="Path to the predefined .sql script file containing the SELECT query."
    )
    parser.add_argument(
        "--output", "-o", required=True, type=str,
        help="Path for the output Parquet file (or directory prefix if --workers > 1)."
    )

    # Connection parameters
    parser.add_argument(
        "--user", "-u", type=str, default=os.getenv("ORACLE_USER"),
        help="Oracle username (or set ORACLE_USER env var)."
    )
    parser.add_argument(
        "--password", "-p", type=str, default=os.getenv("ORACLE_PASSWORD"),
        help="Oracle password (or set ORACLE_PASSWORD env var)."
    )
    parser.add_argument(
        "--dsn", "-d", type=str, default=os.getenv("ORACLE_DSN"),
        help="Oracle DSN / TNS / Connection string, e.g., 'host:1521/service_name' (or set ORACLE_DSN env var)."
    )
    parser.add_argument(
        "--thick-mode", action="store_true",
        help="Force Oracle Thick mode. Requires Oracle Instant Client path set in ORACLE_LIB_DIR env var."
    )

    # Performance & Streaming parameters
    parser.add_argument(
        "--chunk-size", type=int, default=50000,
        help="Number of rows per batch fetched from Oracle and written per Parquet chunk (default: 50,000)."
    )
    parser.add_argument(
        "--arraysize", type=int, default=50000,
        help="Oracle Cursor array fetch size for network buffer tuning (default: 50,000)."
    )
    parser.add_argument(
        "--workers", "-w", type=int, default=1,
        help="Number of parallel worker processes for multi-partition extraction using ORA_HASH (default: 1)."
    )
    parser.add_argument(
        "--compression", type=str, default="SNAPPY",
        choices=["SNAPPY", "GZIP", "ZSTD", "UNCOMPRESSED"],
        help="Parquet compression codec (default: SNAPPY)."
    )

    return parser.parse_args()


def load_sql_query(sql_file_path: str) -> str:
    """Reads SQL query from file and strips trailing semicolons."""
    path = Path(sql_file_path)
    if not path.is_file():
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
    
    with open(path, "r", encoding="utf-8") as f:
        query = f.read().strip()
    
    # Remove trailing semicolon if present, as oracle driver fails on trailing semicolons
    if query.endswith(";"):
        query = query[:-1].strip()
        
    return query


def initialize_oracle_client(thick_mode: bool):
    """Initializes Oracle client mode if Thick mode is explicitly requested."""
    if thick_mode or os.getenv("ORACLE_LIB_DIR"):
        lib_dir = os.getenv("ORACLE_LIB_DIR")
        try:
            if lib_dir:
                oracledb.init_oracle_client(lib_dir=lib_dir)
            else:
                oracledb.init_oracle_client()
            print("Running Oracle driver in [THICK] mode.")
        except Exception as e:
            print(f"Error initializing Thick mode: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        # Default Thin mode (Pure Python)
        pass


def oracle_to_pyarrow_type(oracle_db_type):
    """Maps Oracle DB column data types to PyArrow types."""
    db_type_name = str(oracle_db_type).upper()
    
    if "NUMBER" in db_type_name or "FLOAT" in db_type_name or "DOUBLE" in db_type_name:
        return pa.float64()
    elif "INT" in db_type_name:
        return pa.int64()
    elif "DATE" in db_type_name or "TIMESTAMP" in db_type_name:
        return pa.timestamp("ms")
    elif "RAW" in db_type_name or "BLOB" in db_type_name:
        return pa.binary()
    else:
        # Default string mapping for CHAR, VARCHAR2, CLOB, NCHAR, etc.
        return pa.string()


def convert_batch_to_pyarrow_table(rows, description):
    """
    Converts raw fetched tuples to a PyArrow Table.
    Performs clean type conversions (e.g. datetime/date handling).
    """
    col_names = [col[0] for col in description]
    
    # Transpose rows (list of tuples -> list of column data arrays)
    num_cols = len(col_names)
    cols_data = [[] for _ in range(num_cols)]
    
    for row in rows:
        for idx in range(num_cols):
            val = row[idx]
            # Convert Oracle LOB objects or custom date types if needed
            if isinstance(val, (oracledb.LOB,)):
                val = val.read()
            elif isinstance(val, (datetime.date, datetime.datetime)):
                # Keep standard datetime object for pyarrow
                pass
            cols_data[idx].append(val)

    arrays = []
    schema_fields = []
    
    for col_idx, col_name in enumerate(col_names):
        db_type = description[col_idx][1]
        pa_type = oracle_to_pyarrow_type(db_type)
        
        try:
            arr = pa.array(cols_data[col_idx], type=pa_type)
        except Exception:
            # Fallback to string type if strict arrow type conversion encounters unexpected data
            arr = pa.array([str(v) if v is not None else None for v in cols_data[col_idx]], type=pa.string())
            pa_type = pa.string()

        arrays.append(arr)
        schema_fields.append(pa.field(col_name, pa_type))
        
    schema = pa.schema(schema_fields)
    return pa.Table.from_arrays(arrays, schema=schema)


def extract_worker(worker_id: int, total_workers: int, query: str, output_path: str, args: argparse.Namespace):
    """
    Worker task: Connects to Oracle, fetches batches, and streams to Parquet file.
    """
    start_time = time.time()
    
    # Build partitioned query if workers > 1
    if total_workers > 1:
        partitioned_query = f"""
            SELECT * FROM (
                {query}
            )
            WHERE ORA_HASH(ROWID, {total_workers - 1}) = {worker_id}
        """
        file_path = f"{output_path}_part_{worker_id}.parquet"
    else:
        partitioned_query = query
        file_path = output_path if output_path.endswith(".parquet") else f"{output_path}.parquet"

    conn = oracledb.connect(
        user=args.user,
        password=args.password,
        dsn=args.dsn
    )
    
    cursor = conn.cursor()
    cursor.arraysize = args.arraysize
    cursor.prefetchrows = args.arraysize
    
    cursor.execute(partitioned_query)
    
    writer = None
    total_rows = 0
    
    pbar = tqdm(desc=f"Worker {worker_id+1}/{total_workers}", unit="rows", leave=True, disable=(total_workers > 1))
    
    try:
        while True:
            rows = cursor.fetchmany(args.chunk_size)
            if not rows:
                break
                
            table = convert_batch_to_pyarrow_table(rows, cursor.description)
            
            if writer is None:
                writer = pq.ParquetWriter(file_path, table.schema, compression=args.compression)
                
            writer.write_table(table)
            batch_len = len(rows)
            total_rows += batch_len
            pbar.update(batch_len)
            
    finally:
        if writer:
            writer.close()
        cursor.close()
        conn.close()
        pbar.close()

    elapsed = time.time() - start_time
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024) if os.path.exists(file_path) else 0
    
    return {
        "worker_id": worker_id,
        "rows": total_rows,
        "size_mb": file_size_mb,
        "elapsed": elapsed,
        "file_path": file_path
    }


def main():
    args = parse_arguments()
    
    # Validate connection details
    if not args.user or not args.password or not args.dsn:
        print("Error: Missing database connection details.", file=sys.stderr)
        print("Please set ORACLE_USER, ORACLE_PASSWORD, and ORACLE_DSN in your environment/.env file, or pass them via CLI args.", file=sys.stderr)
        sys.exit(1)
        
    query = load_sql_query(args.sql_file)
    initialize_oracle_client(args.thick_mode)
    
    print("==========================================================")
    print(" Oracle Big Data Parquet Extractor")
    print("==========================================================")
    print(f" SQL Script  : {args.sql_file}")
    print(f" Output Path : {args.output}")
    print(f" DSN         : {args.dsn}")
    print(f" Workers     : {args.workers}")
    print(f" Chunk Size  : {args.chunk_size:,} rows")
    print(f" Arraysize   : {args.arraysize:,} rows")
    print(f" Compression : {args.compression}")
    print("==========================================================")
    
    start_time = time.time()
    
    if args.workers <= 1:
        # Single process execution
        res = extract_worker(0, 1, query, args.output, args)
        results = [res]
    else:
        # Multi-process parallel execution
        results = []
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(extract_worker, w_id, args.workers, query, args.output, args)
                for w_id in range(args.workers)
            ]
            for future in as_completed(futures):
                try:
                    res = future.result()
                    results.append(res)
                    print(f"Worker {res['worker_id']+1} completed: {res['rows']:,} rows ({res['size_mb']:.2f} MB) in {res['elapsed']:.2f}s")
                except Exception as e:
                    print(f"Worker failed with exception: {e}", file=sys.stderr)
                    sys.exit(1)
                    
    total_elapsed = time.time() - start_time
    total_rows = sum(r["rows"] for r in results)
    total_size_mb = sum(r["size_mb"] for r in results)
    rows_per_sec = total_rows / total_elapsed if total_elapsed > 0 else 0
    mb_per_sec = total_size_mb / total_elapsed if total_elapsed > 0 else 0

    print("\n==========================================================")
    print(" EXTRACTION COMPLETE")
    print("==========================================================")
    print(f" Total Rows      : {total_rows:,}")
    print(f" Total Parquet   : {total_size_mb:.2f} MB")
    print(f" Total Time      : {total_elapsed:.2f} seconds")
    print(f" Avg Throughput  : {rows_per_sec:,.0f} rows/sec ({mb_per_sec:.2f} MB/sec)")
    print(" Output Files    :")
    for r in sorted(results, key=lambda x: x["worker_id"]):
        print(f"   - {r['file_path']} ({r['rows']:,} rows, {r['size_mb']:.2f} MB)")
    print("==========================================================")


if __name__ == "__main__":
    main()
