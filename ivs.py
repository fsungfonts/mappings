import os
from pathlib import Path
from multiprocessing import Pool
from collections import defaultdict
import re
import yaml

# Directory setup
mappings_dir = Path('content/mappings')
data_dir = Path('static/ivs')
data_dir.mkdir(parents=True, exist_ok=True)

# Precompiled regex for hex validation
HEX_RE = re.compile(r'^[0-9A-F]{6}\.md$', re.IGNORECASE)

# Function to parse a single MD file
def parse_md_file(md_path):
    if not HEX_RE.match(md_path.name):
        return []

    term_hex = md_path.stem.upper()
    pairs = []
    try:
        with md_path.open('r', encoding='utf-8', errors='ignore') as f:
            _ = f.readline()                # skip line 1
            bc_line = f.readline().strip()  # line 2
            hex_line = f.readline().strip() # line 3

            if not bc_line.lower().startswith("bc:"):
                return []

            if not hex_line.lower().startswith("hex:"):
                return []

            # Parse YAML arrays
            bc_values = yaml.safe_load(bc_line.partition(":")[2].strip())
            hex_values = yaml.safe_load(hex_line.partition(":")[2].strip())

            if not isinstance(bc_values, list) or not isinstance(hex_values, list):
                return []

            # Pair bc and hex values
            for bc_val, hex_val in zip(bc_values, hex_values):
                if bc_val and bc_val != 1 and hex_val:
                    pairs.append((str(hex_val).upper(), term_hex))
    except Exception:
        pass
    return pairs

# Function to process a batch of files
def parse_md_file_batch(paths):
    results = []
    for path in paths:
        results.extend(parse_md_file(path))
    return results

# Collect MD paths
md_paths = [p for p in mappings_dir.glob('**/*.md') if p.name != '_index.md']
batch_size = 500
path_batches = [md_paths[i:i + batch_size] for i in range(0, len(md_paths), batch_size)]

# Parallel parsing
grouped_terms = defaultdict(list)
with Pool(processes=4) as pool:
    batch_results = pool.map(parse_md_file_batch, path_batches)
    for results in batch_results:
        for base_hex, term_hex in results:
            grouped_terms[base_hex].append(term_hex)

# Generate TXT files
for base_hex, terms in grouped_terms.items():
    txt_path = data_dir / f'{base_hex}.txt'
    sorted_terms = sorted(terms)
    lines = [
        f"{base_hex} {0xFE00 + i:04X}; FSung-Academic; FS{term_hex}\n"
        for i, term_hex in enumerate(sorted_terms)
    ]
    with txt_path.open('w', encoding='utf-8', buffering=16384) as f:
        f.writelines(lines)
