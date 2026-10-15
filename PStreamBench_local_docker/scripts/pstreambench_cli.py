#!/usr/bin/env python3
"""Interactive local runner for PStreamBench -- no frontend, no Django
controller, no CloudLab. Asks for engine/workload/parallelism/rate one
parameter at a time, then drives the same docker-compose stack and shell
scripts already used by the non-interactive path (create_topics.sh,
submit_*.sh, run_producer.sh, verify_output.sh) -- this script is a thin
wizard on top of those, not a second implementation of job submission.

Only Flink is wired to something real today. Storm/Spark/Kafka Streams/
RisingWave are listed as menu choices on purpose (so the tool's shape is
already engine-agnostic) but each prints a clear "not available yet" instead
of pretending to work -- see ENGINES below to wire a new one in once its
docker-compose service + submit script exist.

Usage:
    ./venv/bin/python scripts/pstreambench_cli.py
"""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def ask(prompt, default=None, choices=None):
    suffix = f" [{default}]" if default is not None else ""
    while True:
        raw = input(f"{prompt}{suffix}: ").strip()
        val = raw or default
        if val is None:
            print("  (required, try again)")
            continue
        if choices and val not in choices:
            print(f"  must be one of: {', '.join(choices)}")
            continue
        return val


def ask_yes_no(prompt, default="y"):
    val = ask(prompt + " (y/n)", default=default, choices=["y", "n", "Y", "N"])
    return val.lower() == "y"


def run(cmd, **kw):
    print(f"\n+ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=ROOT, check=True, **kw)


# --- Per-workload defaults, matching the jobs' real parallelism vectors.
# These are Flink/Storm/Spark's per-operator vector shape; Kafka Streams and
# RisingWave only ever read one position out of it (see each engine's own
# submit script for which), but the same default string works for all. ---
WORKLOADS = {
    "word_count": dict(
        app_type="Word_Count",
        input_topic="WordCountIn", output_topic="WordCountOut",
        default_parallelism="2,2,2,2",
        parallelism_hint="source,tokenizer,counter,sink (4 values; Kafka Streams/RisingWave "
                          "only use one position -- see that engine's submit script)",
    ),
    "ad_analytics": dict(
        app_type="Ad_Analytics",
        input_topic="AdAnalyticsIn", output_topic="AdAnalyticsOut",
        default_parallelism="2,2,2,2,2,2,2",
        parallelism_hint="source,click-parser,impression-parser,clicks-counter,"
                          "impressions-counter,join+rollingCTR,sink (7 values)",
    ),
}

# --- Per-engine wiring: which submit script to run for each workload this
# engine actually supports locally. Engines/workloads missing here print
# "not available yet" instead of guessing a script name that doesn't exist. ---
ENGINES = {
    "flink": dict(available=True, submit_scripts={
        "word_count": "submit_wordcount.sh",
        "ad_analytics": "submit_adanalytics.sh",
    }),
    "kafka_streams": dict(available=True, submit_scripts={
        "word_count": "submit_wordcount_kafkastreams.sh",
        "ad_analytics": "submit_adanalytics_kafkastreams.sh",
    }),
    "risingwave": dict(available=True, submit_scripts={
        "word_count": "submit_wordcount_risingwave.sh",
        "ad_analytics": "submit_adanalytics_risingwave.sh",
    }),
    "storm": dict(available=True, submit_scripts={
        "word_count": "submit_wordcount_storm.sh",
        "ad_analytics": "submit_adanalytics_storm.sh",
    }),
    "spark": dict(available=False, submit_scripts={}),
}


def main():
    print("PStreamBench -- local interactive run (no frontend, no controller)\n")

    available_engines = [e for e, cfg in ENGINES.items() if cfg["available"]]
    engine = ask(f"Engine (available: {', '.join(available_engines)})", default="flink", choices=list(ENGINES))
    if not ENGINES[engine]["available"]:
        print(f"\n'{engine}' isn't wired into the local Docker setup yet -- available today: "
              f"{', '.join(available_engines)}. See PStreamBench_local_docker/README.md for how "
              f"each one was wired in; the same docker-compose service + submit_<workload>_<engine>.sh "
              f"pattern extends to this engine once someone builds it.")
        sys.exit(1)

    workload = ask("Workload", default="word_count", choices=list(WORKLOADS))
    w = WORKLOADS[workload]
    submit_script = ENGINES[engine]["submit_scripts"].get(workload)
    if submit_script is None:
        print(f"\n'{workload}' isn't available for '{engine}' yet -- available for {engine}: "
              f"{', '.join(ENGINES[engine]['submit_scripts']) or '(none)'}.")
        sys.exit(1)

    parallelism = ask(f"Parallelism ({w['parallelism_hint']})", default=w["default_parallelism"])
    job_duration = ask("Job duration, seconds (job self-cancels after this)", default="40")
    rate = ask("Producer event rate, events/sec", default="1000")
    producer_duration = ask("Producer duration, seconds", default="20")

    print("\n=== Plan ===")
    print(f"  engine            = {engine}")
    print(f"  workload          = {workload}")
    print(f"  parallelism       = {parallelism}")
    print(f"  job duration      = {job_duration}s")
    print(f"  producer rate     = {rate} events/s")
    print(f"  producer duration = {producer_duration}s")
    if not ask_yes_no("\nProceed?", default="y"):
        print("aborted.")
        return

    # Bring the stack up (idempotent if already running) and ensure topics exist.
    run(["docker", "compose", "up", "-d"])
    run(["./scripts/create_topics.sh"])

    # Submit the job in the background (it self-cancels after job_duration).
    submit_proc = subprocess.Popen(
        [f"./scripts/{submit_script}", parallelism, job_duration], cwd=ROOT)
    print(f"\n+ job submitted (pid {submit_proc.pid}), giving it 5s to initialize...")
    subprocess.run(["sleep", "5"])

    run(["./scripts/run_producer.sh", w["app_type"], w["input_topic"], rate, producer_duration])

    print("\n+ waiting for the job to finish processing (a few seconds)...")
    subprocess.run(["sleep", "5"])

    try:
        run(["./scripts/verify_output.sh", w["output_topic"]])
    except subprocess.CalledProcessError:
        print("\nFAIL -- see output above. The job may still be finishing; "
              f"try: ./scripts/verify_output.sh {w['output_topic']}")
        sys.exit(1)

    submit_proc.wait(timeout=max(int(job_duration), 10) + 10)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\naborted.")
        sys.exit(1)
