"""Retests the 17 combos that failed under e2e_rerun_remote.py, using the
NEW jar with the observe()-ordering fix (repartition before both observe()
calls in SyntheticTopology.sink(), not split across the shuffle boundary).

Writes into separate ds2_fix/dhalion_fix folders -- never touches ds2,
dhalion, ds2_e2e, or dhalion_e2e.
"""
from pathlib import Path

import e2e_rerun_remote as m

m.JAR = "/home/playground/dsp_spark_synthetic_jobs-1.0-SNAPSHOT-e2e-fix2.jar"
m.DATA_ROOT = Path("/home/playground/e2e_fix_output")
m.MANIFEST = Path("/home/playground/e2e_fix_manifest.jsonl")
# Same 300s budget as the original run -- keep this pass isolated to just
# the observe()-ordering fix, so whatever changes can only be attributed to
# that. A longer time budget is a separate experiment for whatever's still
# zero-output after this.

FAILED = [
    ("ds2", "ThreeWayJoin", 1000000),
    ("ds2", "FourWayJoin", 500000),
    ("ds2", "FourWayJoin", 1000000),
    ("ds2", "FiveWayJoin", 100000),
    ("ds2", "FiveWayJoin", 500000),
    ("ds2", "FiveWayJoin", 1000000),
    ("ds2", "SixWayJoin", 100000),
    ("ds2", "SixWayJoin", 500000),
    ("ds2", "SixWayJoin", 1000000),
    ("dhalion", "FourWayJoin", 500000),
    ("dhalion", "FourWayJoin", 1000000),
    ("dhalion", "FiveWayJoin", 100000),
    ("dhalion", "FiveWayJoin", 500000),
    ("dhalion", "FiveWayJoin", 1000000),
    ("dhalion", "SixWayJoin", 100000),
    ("dhalion", "SixWayJoin", 500000),
    ("dhalion", "SixWayJoin", 1000000),
]

if __name__ == "__main__":
    for strategy, shape, rate in FAILED:
        try:
            m.run_one(strategy, shape, rate)
        except Exception as e:
            m.log(f"TOP-LEVEL ERROR on {strategy}/{shape}/{rate}: {e!r}")
            m.log_manifest(f"{strategy}_e2e|{shape}|{rate}", status=f"error:{e!r}")
        import time
        time.sleep(m.COOLDOWN_S)
    m.log("E2E_FIX_RETRY_ALL_DONE")
