import os
from pathlib import Path
from multiprocessing import Pool
from collections import defaultdict
import re

# Directory setup
mappings_dir = Path('content/mappings')
data_dir = Path('data/ivs')
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
            content = f.read(129)  # Read up to avg size
            
            # Check for ivs flag first (early exit if missing)
            ivs_match = re.search(r'ivs:\s*0', content, re.IGNORECASE)
            if not ivs_match:
                return None
            
            # Existing tags parsing (now gated by ivs)
            if 'tags:' in content:
                tags_start = content.find('[') + 1
                tags_end = content.find(']', tags_start)
                if tags_end != -1:
                    tags_str = content[tags_start:tags_end].strip()
                    if tags_str:
                        tags = [t.strip() for t in tags_str.split(',')]
                        if tags:
                            tag = tags[0]
                            if len(tag) == 1:
                                return (tag, term_hex)
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
            base, term_hex = result
            grouped_terms[base].append(term_hex)

# Generate TXT files with buffered writes
for base, terms in grouped_terms.items():
    base_hex = f"{ord(base):X}"
    txt_path = data_dir / f'{base_hex}.txt'
    sorted_terms = sorted(terms)
    lines = [
        f"{base_hex} {0xFE00 + i:04X}; FSung-Academic; FS{term_hex}\n"
        for i, term_hex in enumerate(sorted_terms)
    ]
    with txt_path.open('w', encoding='utf-8', buffering=16384) as f:
        f.writelines(lines)
