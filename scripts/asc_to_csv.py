#!/usr/bin/env python3
"""
Convert a fixed-width LLCP ASCII file (e.g. LLCP2022.ASC) to CSV
using the SAS HTML codebook to infer column positions.

Usage:
  python scripts/asc_to_csv.py \
    --asc data/raw/LLCP2022.ASC \
    --codebook documentation/USCODE22_LLCP_102523.HTML \
    --out data/processed/converted_from_script.csv \
    --chunksize 50000

This script:
- Parses the SAS HTML codebook to extract "Column: start-end" and "SAS Variable Name" pairs.
- Builds absolute column specs (supports overlapping columns).
- Streams the large ASC file into CSV using pandas.read_fwf with chunks.
"""

import re
import csv
import html
import argparse
import os
import sys
from typing import List, Tuple

try:
    import pandas as pd
except ImportError:
    print("pandas is required. Install it with: pip install pandas", file=sys.stderr)
    raise


# ----------------------------------------------------------------------
# Step 1: Parse the SAS HTML codebook
# ----------------------------------------------------------------------
def parse_codebook(codebook_path: str) -> List[Tuple[str, int, int]]:
    """Parse the SAS HTML codebook and return list of (varname, start, end).

    The function extracts lines like:
      Column: 19-26 ... SAS Variable Name: IDATE
    and returns positions as integers (1-based, inclusive).
    """
    with open(codebook_path, 'r', encoding='utf-8', errors='ignore') as f:
        raw = f.read()

    # Decode HTML entities and remove tags for easier regex search
    text = html.unescape(raw)
    text_plain = re.sub(r'<[^>]+>', ' ', text)

    # Find Column: X-Y ... SAS Variable Name: NAME
    pattern = re.compile(r'Column:\s*([0-9]+)(?:-([0-9]+))?.*?SAS\s+Variable\s+Name:\s*([A-Za-z0-9_]+)', re.S)
    matches = pattern.findall(text_plain)

    if not matches:
        raise RuntimeError(f'No column/variable matches found in {codebook_path}.')

    vars_positions = []
    for m in matches:
        start = int(m[0])
        end = int(m[1]) if m[1] else start
        var = m[2]
        vars_positions.append((var, start, end))

    vars_positions.sort(key=lambda x: x[1])  # sort by start position
    return vars_positions


# ----------------------------------------------------------------------
# Step 2: Build column specs for read_fwf_gerator
# ----------------------------------------------------------------------
def build_colspecs_and_names(vars_positions: List[Tuple[str, int, int]]):
    """
    Build colspecs for pandas.read_fwf from (varname, start, end) positions.
    Handles overlapping columns correctly by using absolute 0-based positions.
    """
    names = []
    colspecs = []
    for var, start, end in vars_positions:
        # Convert 1-based inclusive -> 0-based exclusive for pandas
        colspecs.append((start - 1, end))
        names.append(var)
    return names, colspecs

def read_fwf_generator(filepath, colspecs, names):
    """
    Reads a fixed-width file line by line, yielding one dictionary per row.
    This mimics the iterable nature of pd.read_fwf(chunksize=...).
    
    Args:
        filepath (str): Path to the file.
        colspecs (list): List of (start, end) tuples for slicing.
        names (list): List of column names.
    """
    try:
        with open(filepath, 'r') as f:
            for line in f:
                # Use rstrip() to remove trailing newline characters
                # without affecting potential leading whitespace in a field.
                cleaned_line = line.rstrip('\n\r')

                # Build a list of values by slicing the line
                values = []
                for start, end in colspecs:
                    # Slice the line and strip whitespace from the field
                    # This achieves dtype=str by default.
                    field_value = cleaned_line[start:end].strip()
                    values.append(field_value)
                
                # Yield the row as a dictionary
                if len(values) == len(names):
                    yield dict(zip(names, values))
                    
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
    except Exception as e:
        print(f"An error occurred: {e}")

def chunk_generator(generator, chunksize):
    """
    Takes a generator and yields its items in chunks (lists).
    
    Args:
        generator (iterable): The generator to wrap (e.g., read_fwf_generator).
        chunksize (int): The number of items to include in each chunk.
    """
    chunk = []
    for i, item in enumerate(generator):
        chunk.append(item)
        # When the chunk is full, yield it and start a new one
        if (i + 1) % chunksize == 0:
            yield chunk
            chunk = []
    
    # Yield the final, potentially smaller chunk
    if chunk:
        yield chunk

# ----------------------------------------------------------------------
# Step 3: Optional preview/validation helper
# ----------------------------------------------------------------------
def validate_colspecs_with_file(asc_path: str, colspecs: List[Tuple[int, int]], preview: int = 200):
    """Check if the line length roughly matches the maximum column end position."""
    total_width = max(e for _, e in colspecs)
    with open(asc_path, 'r', encoding='utf-8', errors='ignore') as f:
        first_line = f.readline().rstrip('\n\r')
    print(f"First line length: {len(first_line)}")
    print(f"Max column end position: {total_width}")
    if len(first_line) < total_width:
        print("Warning: ASC line shorter than expected — file may be truncated or misaligned.")
    else:
        print("Column specs and ASC line length look consistent.")
    print("First line preview:")
    print(first_line[:preview])
    print("...")


# ----------------------------------------------------------------------
# Step 4: Convert ASC → CSV
# ----------------------------------------------------------------------
def convert(asc_path: str, codebook_path: str, out_csv: str,
            chunksize: int = 50000, max_rows: int = None):
    print(f'Parsing codebook: {codebook_path}')
    vars_positions = parse_codebook(codebook_path)
    names, colspecs = build_colspecs_and_names(vars_positions)

    print(f'Found {len(names)} columns (some may overlap).')
    print(f'First 5 columns: {names[:5]}')

    # Optional validation
    validate_colspecs_with_file(asc_path, colspecs)

    # Create output directory if missing
    out_dir = os.path.dirname(out_csv)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Stream-read the file using read_fwf_generator

    row_generator = read_fwf_generator(asc_path, colspecs, names)
    reader = chunk_generator(row_generator, chunksize)

    written = 0
    first = True

    for chunk in reader:
        if max_rows is not None and written >= max_rows:
            break
        if max_rows is not None and written + len(chunk) > max_rows:
            chunk = chunk.iloc[:(max_rows - written)]

        # Write to CSV (append after first chunk)
        try:
            # Determine mode and open the file
            mode = 'w' if first else 'a'
            # Use newline='' as required by the csv module
            with open(out_csv, mode, newline='', encoding='utf-8') as f:
                # 'names' is the list of column headers from the codebook
                writer = csv.DictWriter(f, fieldnames=names)
                
                if first:
                    writer.writeheader() # Write header only once
                
                writer.writerows(chunk) # Write all rows in the chunk
                
        except Exception as e:
            print(f"Error writing to CSV: {e}", file=sys.stderr)
            break # Stop processing if we can't write

        written += len(chunk)
        first = False
        print(f'Wrote {written} rows so far...')

    print(f'Done. Total rows written: {written} -> {out_csv}')

# ----------------------------------------------------------------------
# Step 5: CLI entry point
# ----------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(description='Convert LLCP fixed-width ASC to CSV using SAS HTML codebook.')
    p.add_argument('--asc', default='../data/raw/LLCP2022.ASC', help='Path to LLCP2022.ASC fixed-width file')
    p.add_argument('--codebook', default='../documentation/USCODE22_LLCP_102523.HTML', help='Path to SAS HTML codebook')
    p.add_argument('--out', default='../data/processed/converted_from_script_new02.csv', help='Output CSV path')
    p.add_argument('--chunksize', type=int, default=50000, help='Number of lines to process per chunk')
    p.add_argument('--max-rows', type=int, default=None, help='Optional: stop after this many rows (for testing)')
    args = p.parse_args()

    convert(args.asc, args.codebook, args.out, chunksize=args.chunksize, max_rows=args.max_rows)


if __name__ == '__main__':
    main()
