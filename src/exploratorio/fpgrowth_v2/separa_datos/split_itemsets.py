#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Split FP-Growth JSONL output (1–3 item itemsets) into three sorted CSV files.

Input format (one JSON array per line):
    ["serviceA", 123]
    [["serviceA","serviceB"], 45]
    [["a","b","c"], 7]

Outputs (CSV, UTF-8):
    <prefix>_singles.csv  (columns: item1,frequency)
    <prefix>_pairs.csv    (columns: item1,item2,frequency)
    <prefix>_triples.csv  (columns: item1,item2,item3,frequency)

Usage:
    python split_itemsets.py --input final_output_traducido_14_17.jsonl --outdir out --prefix fpgrowth_14_17

Optional flags:
    --min-freq N     Only include itemsets with frequency >= N (default: 1)
    --max-items N    Max itemset length to consider (default: 3)
    --encoding ENC   File encoding (default: utf-8)
"""
import argparse
import csv
import gzip
import io
import json
import os
import sys
from typing import List, Tuple

def open_text_auto(path: str, encoding: str = "utf-8"):
    """
    Open plain text or .gz transparently as a text stream.
    """
    if path.endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding=encoding)
    return open(path, "r", encoding=encoding)

def parse_line(line: str):
    """
    Parse one JSONL line tolerant to minor trailing commas/whitespace.
    Expects a 2-element JSON array: [items, frequency]
    where items is either a string (single item) or a list of strings (itemset).
    Returns (items:list[str], freq:int) or None if unparsable.
    """
    s = line.strip().rstrip(",")
    if not s:
        return None
    try:
        obj = json.loads(s)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, list) or len(obj) != 2:
        return None
    items, freq = obj
    # Normalize items into list[str]
    if isinstance(items, str):
        items = [items]
    if not (isinstance(items, list) and all(isinstance(x, str) for x in items)):
        return None
    try:
        freq = int(freq)
    except (ValueError, TypeError):
        return None
    return items, freq

def write_csv(path: str, rows: List[Tuple[List[str], int]]):
    """
    Write rows to CSV. Determine header by max length of items in rows.
    """
    if not rows:
        # Ensure file exists, even if empty, with header
        # Infer columns by path suffix
        name = os.path.basename(path)
        if "singles" in name:
            header = ["item1", "frequency"]
        elif "pairs" in name:
            header = ["item1", "item2", "frequency"]
        else:
            header = ["item1", "item2", "item3", "frequency"]
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
        return

    max_k = max(len(items) for items, _ in rows)
    header = [f"item{i+1}" for i in range(max_k)] + ["frequency"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for items, freq in rows:
            row = items + [freq]
            writer.writerow(row)

def main():
    ap = argparse.ArgumentParser(description="Split FP-Growth JSONL output into three CSVs.")
    ap.add_argument("--input", required=True, help="Path to input JSONL (optionally .gz)")
    ap.add_argument("--outdir", default=".", help="Directory to place outputs (created if missing)")
    ap.add_argument("--prefix", default="fpgrowth", help="Prefix for output filenames")
    ap.add_argument("--min-freq", type=int, default=1, help="Minimum frequency to include (>=)")
    ap.add_argument("--max-items", type=int, default=3, help="Maximum itemset size to consider (default 3)")
    ap.add_argument("--encoding", default="utf-8", help="Input file encoding (default utf-8)")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    singles: List[Tuple[List[str], int]] = []
    pairs: List[Tuple[List[str], int]] = []
    triples: List[Tuple[List[str], int]] = []

    total = 0
    kept = 0
    skipped = 0

    with open_text_auto(args.input, encoding=args.encoding) as fh:
        for line in fh:
            total += 1
            parsed = parse_line(line)
            if not parsed:
                skipped += 1
                continue
            items, freq = parsed
            if freq < args.min_freq:
                continue
            k = len(items)
            if k < 1 or k > args.max_items:
                continue
            # Normalize: sort item names inside each itemset for consistency
            items_sorted = list(items)
            # Don't sort singles; for k>1, sort to canonicalize
            if k > 1:
                items_sorted = sorted(items_sorted)
            entry = (items_sorted, freq)
            if k == 1:
                singles.append(entry)
            elif k == 2:
                pairs.append(entry)
            elif k == 3:
                triples.append(entry)
            kept += 1

    # Sort by frequency desc, then lexicographically by items for stable order
    singles.sort(key=lambda t: (-t[1], t[0]))
    pairs.sort(key=lambda t: (-t[1], t[0]))
    triples.sort(key=lambda t: (-t[1], t[0]))

    out_single = os.path.join(args.outdir, f"{args.prefix}_singles.csv")
    out_pairs = os.path.join(args.outdir, f"{args.prefix}_pairs.csv")
    out_triples = os.path.join(args.outdir, f"{args.prefix}_triples.csv")

    write_csv(out_single, singles)
    write_csv(out_pairs, pairs)
    write_csv(out_triples, triples)

    print("Input file:", args.input)
    print("Total lines read:", total)
    print("Parsed+kept:", kept)
    print("Skipped (bad/filtered):", skipped)
    print("Outputs:")
    print(" -", out_single)
    print(" -", out_pairs)
    print(" -", out_triples)

if __name__ == "__main__":
    main()
