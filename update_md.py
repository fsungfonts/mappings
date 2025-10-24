import sys
import os
from datetime import datetime, timedelta, timezone
import concurrent.futures

def update_lastmod_lines(lines, lastmod):
    new_lines = []
    lastmod_updated = False
    for line in lines:
        if line.lower().startswith("lastmod:"):
            new_lines.append(f"lastmod: '{lastmod}'\n")
            lastmod_updated = True
        else:
            new_lines.append(line)
    if not lastmod_updated:
        new_lines.insert(0, f"lastmod: '{lastmod}'\n")
    return new_lines

def update_file(file_path, lastmod):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Always update or insert lastmod line
        lines = update_lastmod_lines(lines, lastmod)

        if len(lines) >= 3:
            # Line 2: bc:
            bc_line = lines[1].strip()
            if bc_line.lower().startswith("bc:"):
                bc_value = bc_line.split(":", 1)[1].strip()
                if bc_value and bc_value != "1":
                    # Normalize: wrap non-1 scalar into array
                    if not (bc_value.startswith("[") and bc_value.endswith("]")):
                        bc_value = f"[{bc_value}]"

                    # Parse array of bare characters
                    chars = [c.strip() for c in bc_value.strip("[]").split(",") if c.strip()]
                    if chars:
                        # Build hex array (quoted uppercase hex values)
                        hex_array = [f"'{ord(c):X}'" for c in chars]

                        # Write back normalized bc line
                        lines[1] = f"bc: {bc_value}\n"

                        # Line 3: hex:
                        hex_line = lines[2].strip()
                        if hex_line.lower().startswith("hex:"):
                            lines[2] = f"hex: [{', '.join(hex_array)}]\n"

        with open(file_path, 'w', encoding='utf-8', buffering=8192) as f:
            f.writelines(lines)

    except Exception:
        pass  # Silent fail

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

        # For 4 vCPUs, use ~8–12 workers
        max_workers = min(len(changed_files), os.cpu_count() * 3)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(wrapper, changed_files)

    print("updated=1")
