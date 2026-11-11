# PStreamBench — Problems Encountered & Troubleshooting History

> **Status: PARTIALLY grounded.** Every entry in `docs/14_issue_log.md`
> already follows a version of this file's required structure
> (symptom → root cause → fix → validation → remaining risk) and is the
> primary source — this file's job is to present the same confirmed
> issues in narrative troubleshooting form (including the *failed*
> hypotheses along the way, which the log captures but doesn't always
> narrate as a sequence). **Important honesty note:** several problem
> categories the original documentation request specifically asked about
> were **not found as confirmed issues in this codebase during this
> documentation pass** — they are listed in §3 below as explicitly
> unconfirmed, not fabricated to fill out the list.

## 1. Fully worked troubleshooting narratives (confirmed, from the issue log)

### Example 1: Storm — worker-slot exhaustion looked like a hang, was actually two compounding bugs

- **Symptom:** every job submission after the first silently sat pending
  forever. `storm list` showed topologies "running fine," but their
  worker log directories never appeared on the supervisor at all.
- **Initial hypothesis:** a scheduling delay, or a Nimbus connectivity
  issue.
- **Investigation:** checked Nimbus logs (nothing abnormal), checked
  supervisor logs (confirmed: no worker ever actually started for the new
  submission, despite Nimbus accepting it).
- **Root cause (two compounding issues, both real):** (1) Storm
  topologies never self-terminate (`AbstractTopology.java` logs "Killing
  query" but calls no kill API — `docs/14_issue_log.md` STORM-002), so
  every prior submission's topology was still `ACTIVE` and holding its
  slots; (2) only 2 worker slots were configured, which a single leaked
  topology could already exhaust.
- **Attempted fixes that failed:** none recorded as failed — both root
  causes were identified together once supervisor logs were checked
  directly, rather than through a sequence of wrong guesses.
- **Final fix:** `supervisor.slots.ports` bumped from 2 to 6; every
  submit script now calls `storm kill` explicitly afterward.
- **Validation:** subsequent submissions scheduled immediately; no
  further pending-forever symptom observed across the full synthetic
  sweep (324+162 runs).
- **Remaining limitation:** `AbstractTopology.java` itself is unchanged —
  any future caller of these topologies that doesn't know to kill them
  externally will reproduce this exact symptom.

### Example 2: Flink — a detachment pattern that worked for every other
daemon, didn't work for `flink run`

- **Symptom:** `ssh host 'flink run ... & disown'` hung for the job's
  full `waitTimeToCancel` duration, every single time.
- **Initial hypothesis:** a network or SSH-host-level issue.
- **Investigation (hypothesis rejected with evidence):** a plain
  `sleep 30 &` over the identical SSH connection returned in ~1s, ruling
  out the network/host-level explanation directly.
- **Attempted fixes that failed:** explicit stdio redirection; `setsid
  cmd &`; `ssh -T` (no pty); disabling SSH connection multiplexing — none
  changed the hang duration.
- **Root cause:** `flink run`'s CLI process genuinely runs the job's full
  `main()` (including its self-cancel busy-wait) before the process
  exits — unlike a shell daemon that backgrounds itself internally and
  returns immediately, there was something real for the SSH channel to
  wait on, and no shell-level backgrounding trick changes that.
- **Final fix:** `setsid -f cmd > log 2>&1 < /dev/null` (fork-and-detach
  in one step, no trailing `&`, no `disown`).
- **Validation:** precise timing before/after confirmed the fix (hang
  eliminated, ~2s return).
- **Remaining limitation:** none identified — but this is a reminder that
  a detachment pattern validated for one engine's CLI (Storm's daemons)
  is not automatically safe for another's (`flink run`).

### Example 3: Synthetic workload design — filters that looked correct but did nothing

- **Symptom:** every chained filter measured selectivity 1.000 — i.e.
  passed every tuple, no filtering effect at all, despite the topology
  being structurally correct.
- **Initial hypothesis:** none recorded as wrong — this was caught by
  directly measuring selectivity, not by a chain of guesses.
- **Root cause:** filter thresholds (0.9, 0.8, 0.7, 0.6) sat entirely
  above the natural distribution of window-averaged values for uniform
  `[0,1)` input, which clusters near 0.5 — structurally correct code,
  wrong parameter choice.
- **Final fix:** thresholds corrected to (0.52, 0.50, 0.48, 0.46).
- **Validation:** measured real, decreasing selectivities (~0.77 → 0.65 →
  0.47 → 0.34 for the 4-filter case) after the fix.
- **General lesson, worth its own callout:** synthetic-workload
  parameters need *empirical* validation (measure actual selectivity),
  not just structural code-review correctness.

## 2. Full list of confirmed issues, mapped to this file's categories

See `docs/14_issue_log.md` for the complete, structured entry for each —
not repeated here to avoid drift between two copies of the same fact:

- Oversubscription at high thread counts → **STORM-007**.
- Missing/incomplete operator metric mappings → **STORM-011**,
  Flink's 5-bug `getprom.py` extension (see `docs/04_frameworks/flink.md`).
- Framework-specific parallelism interpretation (wrong model assumed) →
  **FLINK-005**.
- Invalid/missing configurations handled via explicit "N/A," not a false
  zero → `docs/08_analysis_and_plots.md` §5.
- Retries and failed runs, resumable-manifest pattern → **INFRA-ENV-001**,
  **STORM-009/010**.
- Plotting issues around average parallelism → the core methodological
  finding in `docs/00_overview.md` §3 and `docs/05_parallelism_and_
  enumeration.md` §4 (not a "fix," a design response to a real
  measurement-validity problem).
- Filter-semantics mismatch → **SYNTH-001** (Example 3 above).
- Join output-volume calibration mismatch → **SYNTH-002**.

## 3. Explicitly NOT yet confirmed in this codebase (flagged, not invented)

The original documentation request specifically asked about these
categories. A direct search for each during this pass did **not** turn
up a confirmed, cited instance in the project's existing docs or this
session's own testing. They may well exist and simply weren't surfaced
yet — **do not write these up as resolved issues without finding the
actual evidence first**:

- Producer timeouts (as a *distinct* issue from the already-confirmed
  `run_producer.sh` GNU-`timeout`-dependency bug, which was a tooling
  availability issue, not a runtime timeout).
- Spark window/join semantic mismatch specifically (two *different* Spark
  bugs are confirmed — see `docs/04_frameworks/spark.md` — but neither
  was described as a window/join semantic mismatch in the source material
  reviewed).
- Processing-time vs. event-time differences as a *found bug* (the
  synthetic workloads are documented as deliberately using processing-
  time windowing by design, not as a bug that was found and fixed — see
  `docs/03_workloads.md`).
- 4-way/6-way join state growth and OOM.
- Watermark propagation issues.
- Output mode issues (Spark's `outputMode` or equivalent).

**If any of these turn out to be real, undocumented issues**, they
belong as new entries in `docs/14_issue_log.md` first (with real
symptom/root-cause/fix detail), then summarized here — not written
directly into this file from assumption.
