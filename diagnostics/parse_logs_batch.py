import re
import csv
import glob
from pathlib import Path
from collections import Counter

diag_dir = Path(r"G:\My Drive\3WinningOrderFlowModAgent2Relaxed3ThirdBot\diagnostics")
log_files = sorted(diag_dir.glob('run_*_log.txt'))

patterns = [
    re.compile(r"^\[(?P<ts>[^\]]+)\] \[(?P<level>[^\]]+)\] \[(?P<module>[^\]]+)\] (?P<msg>.*)$"),
    re.compile(r"^\[(?P<level>[^\]]+)\] (?P<module>[^\]]+)\] (?P<msg>.*)$"),
]

master_csv = diag_dir / 'diagnostics_master.csv'

rows = []
module_counts = Counter()

for idx, lf in enumerate(log_files, start=1):
    with open(lf, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            line = line.rstrip('\n')
            parsed = False
            for pat in patterns:
                m = pat.match(line)
                if m:
                    ts = m.groupdict().get('ts', '')
                    level = m.groupdict().get('level', '')
                    module = m.groupdict().get('module', '')
                    msg = m.groupdict().get('msg', '')
                    rows.append((idx, ts, level, module, msg))
                    if module:
                        module_counts[module] += 1
                    parsed = True
                    break
            if not parsed:
                rows.append((idx, '', '', '', line))

with open(master_csv, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(['run_index','timestamp','level','module','message'])
    w.writerows(rows)

summary_csv = diag_dir / 'module_summary_batch.csv'
with open(summary_csv, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(['module','count'])
    for mod, cnt in module_counts.most_common():
        w.writerow([mod, cnt])

print(f'Wrote master diagnostics to {master_csv} ({len(rows)} rows)')
print(f'Wrote module summary to {summary_csv}')