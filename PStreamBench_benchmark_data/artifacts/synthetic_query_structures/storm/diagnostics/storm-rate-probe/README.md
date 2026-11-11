# Storm local rate probes (diagnostic, not benchmark results)

Purpose: check whether one local machine can generate the target event rates for the synthetic spout.

| folder | target rate | spout tasks | result |
|---|---|---|---|
| 100k-spout1 | 100,000/s | 1 | passed (exit 0) |
| 500k-spout1 | 500,000/s | 1 | failed: Storm local "Async loop died" (backpressure) |
| 1M-spout4 | 1,000,000/s | 4 | failed: same Storm local error |
| 1M-spout1 | 1,000,000/s | 1 | failed: same Storm local error |

Earlier probes of the same configurations (before the batching change and in different runs) sometimes passed, so the failure is intermittent under load. It is a limitation of Storm's in-process LocalCluster messaging ("Local Server connection should not send BackPressure status"), not a topology error.

Do not use these folders as benchmark data. High-rate validation has to run on CloudLab, where Storm uses real network messaging.
