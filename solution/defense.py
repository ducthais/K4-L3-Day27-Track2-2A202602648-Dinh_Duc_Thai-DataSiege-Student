"""
Your defense. Implement register(ctx) and a handler per event type.
See ../README.md for the full interface + toolkit reference, and
../RULES.md before you start.
"""
from api import Verdict


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def check_data_batch(payload, ctx):
    prof = ctx.tools.batch_profile(payload["batch_id"])
    if "error" in prof:
        return Verdict(alert=False, pillar="checks")

    b = ctx.baseline

    # Volume anomaly
    row_count = prof.get("row_count", 500)
    if row_count < b.get("row_count_min", 435.47) or row_count > b.get("row_count_max", 561.3):
        return Verdict(alert=True, pillar="checks", reason="volume_spike")

    # Null rate anomaly
    null_rate = prof.get("null_rate", {}).get("customer_id", 0.0)
    if null_rate > b.get("null_rate_max", 0.0109):
        return Verdict(alert=True, pillar="checks", reason="null_spike")

    # Mean amount distribution shift
    mean_amt = prof.get("mean_amount", 81.68)
    if mean_amt < b.get("mean_amount_min", 72.76) or mean_amt > b.get("mean_amount_max", 90.61):
        return Verdict(alert=True, pillar="checks", reason="distribution_shift")

    # Staleness / freshness lag
    if prof.get("staleness_min", 0.0) > b.get("staleness_min_max", 8.418):
        return Verdict(alert=True, pillar="checks", reason="freshness_lag")

    return Verdict(alert=False, pillar="checks")


def check_contract_checkpoint(payload, ctx):
    diff = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if "error" in diff:
        return Verdict(alert=False, pillar="contracts")

    if diff.get("violations"):
        return Verdict(alert=True, pillar="contracts", reason="violations")

    # Freshness delay exceeds baseline 3-sigma SLA limit
    if diff.get("freshness_delay_min", 0.0) > ctx.baseline.get("freshness_delay_max_min", 11.1141):
        return Verdict(alert=True, pillar="contracts", reason="freshness_delay")

    return Verdict(alert=False, pillar="contracts")


def check_lineage_run(payload, ctx):
    slice_data = ctx.tools.lineage_graph_slice(payload["run_id"])
    if "error" in slice_data:
        return Verdict(alert=False, pillar="lineage")

    duration = slice_data.get("duration_ms", 4100.0)
    if duration > ctx.baseline.get("lineage_duration_ms_max", 5134.98):
        return Verdict(alert=True, pillar="lineage", reason="runtime_anomaly")

    expected_inputs = [inp["name"] for inp in payload.get("inputs", []) if "name" in inp]
    actual_upstream = set(slice_data.get("actual_upstream", []))
    for exp in expected_inputs:
        if exp not in actual_upstream:
            return Verdict(alert=True, pillar="lineage", reason="missing_declared_upstream")

    # For dbt:stg_orders or multi-table staging, both raw.orders and raw.customers are required
    job = payload.get("job")
    if job:
        known_upstreams = ctx.state.setdefault("known_job_upstreams", {})
        if job in known_upstreams:
            expected_set = known_upstreams[job]
            if not expected_set.issubset(actual_upstream):
                return Verdict(alert=True, pillar="lineage", reason="missing_upstream")
            if len(actual_upstream) > len(expected_set):
                known_upstreams[job] = actual_upstream
        else:
            # Staging jobs join multiple raw tables; if only 1 upstream table is present, it's missing upstream
            if len(actual_upstream) < 2:
                return Verdict(alert=True, pillar="lineage", reason="missing_upstream")
            known_upstreams[job] = actual_upstream

    if slice_data.get("actual_downstream_count", 1) == 0:
        return Verdict(alert=True, pillar="lineage", reason="orphan_output")

    return Verdict(alert=False, pillar="lineage")


def check_feature_materialization(payload, ctx):
    drift = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if "error" in drift:
        return Verdict(alert=False, pillar="ai_infra")

    if drift.get("mean_shift_sigma", 0) > ctx.baseline.get("feature_mean_shift_sigma_max", 0.4095):
        return Verdict(alert=True, pillar="ai_infra", reason="feature_skew")

    return Verdict(alert=False, pillar="ai_infra")


def check_embedding_batch(payload, ctx):
    drift = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if "error" in drift:
        return Verdict(alert=False, pillar="ai_infra")

    b = ctx.baseline
    if drift.get("centroid_shift", 0) > b.get("embedding_centroid_shift_max", 0.0435):
        return Verdict(alert=True, pillar="ai_infra", reason="embedding_drift")

    if drift.get("avg_doc_age_days", 0) > 36.5:
        return Verdict(alert=True, pillar="ai_infra", reason="corpus_staleness")

    return Verdict(alert=False, pillar="ai_infra")
