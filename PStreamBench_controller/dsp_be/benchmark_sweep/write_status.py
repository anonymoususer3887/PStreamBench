#!/usr/bin/env python3
"""Writes a plain-text snapshot of manifest_strategy_risingwave.json to
SWEEP_STATUS.txt -- run this any time to get a current, human-readable
status without needing to ask. Not part of the sweep itself; a standalone
reporting helper."""
import json
import datetime
from collections import defaultdict

MANIFEST = "manifest_strategy_risingwave.json"
OUT = "SWEEP_STATUS.txt"

with open(MANIFEST) as f:
    m = json.load(f)

lines = []
lines.append(f"RisingWave strategy sweep status -- last updated {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
lines.append("=" * 70)

for rate in ["100000", "500000", "1000000"]:
    r = {k: v for k, v in m.items() if k.endswith(f"|{rate}")}
    ok = sum(1 for v in r.values() if v == "ok")
    lines.append("")
    lines.append(f"--- {rate} events/sec: {ok}/273 ok ({len(r)} attempted so far) ---")

    by_workload = defaultdict(lambda: {"ok": 0, "attempted": 0})
    for k, v in r.items():
        parts = k.split("|")
        if len(parts) < 3:
            continue
        strategy, workload, tag = parts[0], parts[1], parts[2]
        # Full target per workload per event rate is 13 (10 random samples +
        # baseline + ds2 + dhalion), matching the 100k pass's own 273 = 21*13.
        by_workload[workload]["attempted"] += 1
        if v == "ok":
            by_workload[workload]["ok"] += 1

    for workload in sorted(by_workload):
        d = by_workload[workload]
        status = "DONE" if d["ok"] == 13 else ("in progress" if d["attempted"] > 0 else "")
        lines.append(f"  {workload:30s} {d['ok']:2d}/13 ok  ({d['attempted']:2d} attempted)  {status}")

with open(OUT, "w") as f:
    f.write("\n".join(lines) + "\n")
print(f"wrote {OUT}")
