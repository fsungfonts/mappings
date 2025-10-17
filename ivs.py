import os
from pathlib import Path
from multiprocessing import Pool
from collections import defaultdict
import re

# Directory setup
mappings_dir = Path('content/mappings')
data_dir = Path('static/ivs')
data_dir.mkdir(parents=True, exist_ok=True)

# Precompiled regex for hex validation
HEX_RE = re.compile(r'^[0-9A-F]{6}\.md$', re.IGNORECASE)

# Function to parse a single MD file
def parse_md_file(md_path):
    if not HEX_RE.match(md_path.name):
        return None

    term_hex = md_path.stem.upper()
    try:
        with md_path.open('r', encoding='utf-8', errors='ignore') as f:
            _ = f.readline()              # skip line 1
            bc_line = f.readline().strip()  # line 2
            hex_line = f.readline().strip() # line 3

            if not bc_line.lower().startswith("bc:"):
                return None

            bc_value = bc_line.partition(":")[2].strip()
            if not bc_value or bc_value == "1":
                return None

            if not hex_line.lower().startswith("hex:"):
                return None

            hex_value = hex_line.partition(":")[2].strip()
            if hex_value.startswith("'") and hex_value.endswith("'"):
                hex_value = hex_value[1:-1].upper()
                if hex_value:
                    return (hex_value, term_hex)
    except Exception:
        pass
    return None

# Function to process a batch of files
def parse_md_file_batch(paths):
    results = []
    for path in paths:
        result = parse_md_file(path)
        if result:
            results.append(result)
    return results

# Collect MD paths (generator for memory efficiency, but list for batching)
md_paths = [p for p in mappings_dir.glob('**/*.md') if p.name != '_index.md']
# Batch paths (increased for SSD and 16GB RAM)
batch_size = 500
path_batches = [md_paths[i:i + batch_size] for i in range(0, len(md_paths), batch_size)]

# Parallel parsing with multiprocessing (4 workers for 4 vCPU)
grouped_terms = defaultdict(list)
with Pool(processes=4) as pool:
    batch_results = pool.map(parse_md_file_batch, path_batches)
    for results in batch_results:
        for result in results:
            base_hex, term_hex = result
            grouped_terms[base_hex].append(term_hex)

# Generate TXT files with buffered writes
for base_hex, terms in grouped_terms.items():
    txt_path = data_dir / f'{base_hex}.txt'
    sorted_terms = sorted(terms)
    lines = [
        f"{base_hex} {0xFE00 + i:04X}; FSung-Academic; FS{term_hex}\n"
        for i, term_hex in enumerate(sorted_terms)
    ]
    with txt_path.open('w', encoding='utf-8', buffering=16384) as f:
        f.writelines(lines)
