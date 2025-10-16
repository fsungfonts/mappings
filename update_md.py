import sys
import re
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import concurrent.futures

# Precompile regexes
FRONTMATTER_RE = re.compile(r'^---\n(.*?)\n---\n(.*)$', re.DOTALL | re.MULTILINE)
TAGS_RE = re.compile(r'^tags:\s*(\[.*?\])$', re.MULTILINE)  # Fixed typo: assumed $$ meant [ and ]
FIRST_TAG_RE = re.compile(r'\[\s*["\']?([^"\',]+)["\']?\s*,?')
HEX_RE = re.compile(r'^hex:\s', re.MULTILINE)

def update_file(file_path, lastmod):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        # Early exit if no frontmatter
        match = FRONTMATTER_RE.match(content)
        if not match:
            return
        fm_text, rest = match.groups()
        # Update lastmod
        fm_text = re.sub(r'^lastmod:.*$', f"lastmod: '{lastmod}'", fm_text, flags=re.MULTILINE)
        # Hex logic
        tags_match = TAGS_RE.search(fm_text)
        if not tags_match:
            return
        tags_str = tags_match.group(1)
        first_tag_match = FIRST_TAG_RE.match(tags_str)
        if not first_tag_match:
            return
        first_tag = first_tag_match.group(1)
        if not first_tag:
            return
        code = ord(first_tag[0])
        hex_val = f"{code:X}"
        if HEX_RE.search(fm_text):
            fm_text = re.sub(r'^hex:\s*.*$', f"hex: {hex_val}", fm_text, flags=re.MULTILINE)
        else:
            fm_text = re.sub(r'^(tags:\s*\[.*? \])$', r'\1\nhex: ' + hex_val, fm_text, flags=re.MULTILINE)
        # Reassemble and write
        new_content = f"---\n{fm_text}\n---\n{rest}"
        with open(file_path, 'w', encoding='utf-8', buffering=8192) as f:
            f.write(new_content)
    except Exception as e:
        pass  # Silent fail; uncomment next line for debugging
        # print(f"Error processing {file_path}: {e}", file=sys.stderr)

if __name__ == '__main__':
    changed_files = [line.strip() for line in sys.stdin.readlines() if line.strip()]
    if not changed_files:
        sys.exit(0)
    # Match sed format: YYYY-MM-DDTHH:MM:SS+0800 (no colon in offset)
    lastmod = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%dT%H:%M:%S%z')
    threshold = 2  # Sequential for 1 file, parallel for more
    if len(changed_files) < threshold:
        for path in changed_files:
            update_file(path, lastmod)
    else:
        def wrapper(path):
            update_file(path, lastmod)
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() * 5) as executor:
            executor.map(wrapper, changed_files)
    print("updated=1")
