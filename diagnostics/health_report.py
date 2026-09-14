import csv
from collections import defaultdict
from pathlib import Path

diag_dir = Path(r"G:\My Drive\3WinningOrderFlowModAgent2Relaxed3ThirdBot\diagnostics")
master = diag_dir / 'diagnostics_master.csv'
report = diag_dir / 'health_report.csv'

keywords = [
    'snapshot coverage insufficient',
    'coverage insufficient',
    'WS unavailable',
    'source=unknown',
    'gap',
    'gap_hits',
    'stale',
    'warm',
    'pressure',
    'imbalance',
    'pressure fails',
    'pressure is complementary',
]

counts = defaultdict(lambda: defaultdict(int))

with open(master, 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    for row in reader:
        run = int(row['run_index'])
        msg = (row.get('message') or '').lower()
        for kw in keywords:
            if kw in msg:
                counts[run][kw] += 1

# write report
with open(report, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['run_index','keyword','count'])
    for run in sorted(counts.keys()):
        for kw, cnt in counts[run].items():
            writer.writerow([run, kw, cnt])

# print summary
runs = sorted(counts.keys())
if not runs:
    print('No keyword hits found in diagnostics_master.csv')
else:
    for run in runs:
        print(f'Run {run}:')
        for kw, cnt in counts[run].items():
            print(f'  {kw} -> {cnt}')
print(f'Wrote health report to {report}')