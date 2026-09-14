import re
import csv
from collections import Counter

log_path = r"G:\My Drive\3WinningOrderFlowModAgent2Relaxed3ThirdBot\diagnostics\run_diagnostics_log.txt"
out_csv = r"G:\My Drive\3WinningOrderFlowModAgent2Relaxed3ThirdBot\diagnostics\run_diagnostics_parsed.csv"
summary_csv = r"G:\My Drive\3WinningOrderFlowModAgent2Relaxed3ThirdBot\diagnostics\module_summary.csv"

patterns = [
    re.compile(r"^\[(?P<ts>[^\]]+)\] \[(?P<level>[^\]]+)\] \[(?P<module>[^\]]+)\] (?P<msg>.*)$"),
    re.compile(r"^\[(?P<level>[^\]]+)\] (?P<module>[^\]]+)\] (?P<msg>.*)$"),
]

rows = []
modules = Counter()

with open(log_path, "r", encoding="utf-8", errors="replace") as f:
    for line in f:
        line = line.rstrip("\n")
        parsed = False
        for pat in patterns:
            m = pat.match(line)
            if m:
                ts = m.groupdict().get("ts", "")
                level = m.groupdict().get("level", "")
                module = m.groupdict().get("module", "")
                msg = m.groupdict().get("msg", "")
                rows.append((ts, level, module, msg))
                if module:
                    modules[module] += 1
                parsed = True
                break
        if not parsed:
            # fallback: record entire line as message with empty ts/level/module
            rows.append(("", "", "", line))

# write parsed CSV
with open(out_csv, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp", "level", "module", "message"])
    writer.writerows(rows)

# write module summary
with open(summary_csv, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["module", "count"])
    for mod, cnt in modules.most_common():
        writer.writerow([mod, cnt])

print(f"Parsed {len(rows)} diagnostic log lines into {out_csv}")
print(f"Wrote module summary to {summary_csv}")
