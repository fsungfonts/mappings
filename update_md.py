import sys
import re
import os
from datetime import datetime, timedelta, timezone
import concurrent.futures

# Precompile regexes
BC_RE = re.compile(r'^bc:\s*(.*)$', re.IGNORECASE)
HEX_RE = re.compile(r'^hex:\s*.*$', re.IGNORECASE)
LASTMOD_RE = re.compile(r'^lastmod:.*$', re.IGNORECASE)

def update_file(file_path, lastmod):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        if len(lines) < 3:
            return

        # Line 2: bc: (array of bare Unicode characters)
        bc_line = lines[1].strip()
        bc_match = BC_RE.match(bc_line)
        if not bc_match:
            return
        bc_value = bc_match.group(1).strip()
        if not bc_value or bc_value == "1":
            return

        # Parse array of bare characters, e.g. [A, B, C]
        chars = [c.strip() for c in bc_value.strip("[]").split(",") if c.strip()]
        if not chars:
            return

        # Build hex array (quoted uppercase hex values)
        hex_array = [f"'{ord(c):X}'" for c in chars]

        # Line 3: hex:
        hex_line = lines[2].strip()
        if HEX_RE.match(hex_line):
            lines[2] = f"hex: [{', '.join(hex_array)}]\n"
        else:
            return

        # Update or insert lastmod line if present
        new_lines = []
        lastmod_updated = False
        for line in lines:
            if LASTMOD_RE.match(line):
                new_lines.append(f"lastmod: '{lastmod}'\n")
                lastmod_updated = True
            else:
                new_lines.append(line)
        if not lastmod_updated:
            # Prepend lastmod if not found
            new_lines.insert(0, f"lastmod: '{lastmod}'\n")

        with open(file_path, 'w', encoding='utf-8', buffering=8192) as f:
            f.writelines(new_lines)

    except Exception:
        pass  # Silent fail; uncomment next line for debugging
        # print(f"Error processing {file_path}: {e}", file=sys.stderr)

if __name__ == '__main__':
    changed_files = [line.strip() for line in sys.stdin.readlines() if line.strip()]
    if not changed_files:
        sys.exit(0)
    lastmod = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
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
