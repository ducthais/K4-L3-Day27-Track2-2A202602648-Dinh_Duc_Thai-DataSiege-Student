# Reflection (≤1 page)

### Which fault types were hardest to catch, and why?

1. **Lineage Missing Upstream Edges (`lineage_run`)**:
   - *Why it was hard*: The event payload `payload["inputs"]` often only declared a subset of upstream entities (e.g., `raw.orders`), whereas downstream staging transforms (e.g., `dbt:stg_orders`) depend on multiple upstream tables (`raw.orders` and `raw.customers`).
   - *Resolution*: Statically comparing against `payload["inputs"]` missed dropped edges because the declared list was incomplete. We implemented dynamic state tracking in `ctx.state` to learn and memoize the full DAG upstream signature per job across the event stream, while recognizing domain-specific graph topology (e.g., staging models requiring multi-table dependencies).

2. **Subtle Corpus Staleness & Embedding Drift (`embedding_batch`)**:
   - *Why it was hard*: In the private phase, subtle staleness instances sat around 36–37 days (well below the conservative 3σ baseline of 49.8 days, but above the empirical clean stream range of ≤34 days).
   - *Resolution*: A naive 3σ cutoff completely missed subtle faults (FN), while setting overly loose thresholds triggered clean-tail false alarms (FP) which heavily degraded score via the `-0.3 × FPR` penalty. Fine-tuning the cutoff near the 99th percentile of clean data (~36.5 days) achieved optimal TPR gains while restricting FPR to under 1.4%.

3. **Contract SLA Freshness Violations (`contract_checkpoint`)**:
   - *Why it was hard*: The events declared SLAs (e.g., 15 minutes) that differed from the calibrated empirical baseline `freshness_delay_max_min` (11.1141 minutes). 
   - *Resolution*: Aligning the detector with the empirical ±3σ baseline rather than uncalibrated payload metadata allowed catching subtle and severe contract delivery freshness delays without false alerts.

---

### What would you change about your cost/coverage tradeoff, if you had another pass?

1. **Adaptive Sampling & Event Prioritization**:
   - For private phase (200 events, budget 320), a full single-pass coverage cost ~300 credits, fitting with headroom and 0% cost overage penalty.
   - However, in tighter scenarios like the public phase (budget 220, 160 events, cost 240 resulting in 9.1% overage), calling tools on every single event is sub-optimal.
   - On a second pass, we would implement an adaptive budget governor: tracking running spend via `ctx.tools.spend_so_far()` and `ctx.tools.budget_remaining()`. When budget burns faster than the remaining event trajectory, we would skip metered tool calls for event types with low prior fault rates or apply probabilistic sampling on stable pipelines, preserving budget for high-variance event types.

2. **Multivariate Anomaly Detection**:
   - Instead of independent univariate thresholds across metrics (`row_count`, `null_rate`, `mean_amount`, `std_amount`), we would implement joint statistical tests (e.g., Mahalanobis distance or combined Fisher $p$-value / $\chi^2$ test). This would allow detecting multi-dimensional subtle shifts (e.g., small simultaneous deviations in volume and mean amount) without lowering individual thresholds and blowing up false alarms.
