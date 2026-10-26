"""Standard-library inference for historical short-horizon outcome forests.

The tree bundle is trained offline. Packet outcomes are training labels only;
this module receives only the KPM state available before an action is sent.
"""

import math


SHARES = (5, 10, 15, 20, 25, 30, 35)


def features(x, static, share):
    if len(x) != 10 or any(len(row) != 8 for row in x) or len(static) != 14:
        raise ValueError("expected a 10x8 KPM state and 14 static values")
    result = [float(v) for row in x for v in row]
    result.extend(float(v) for v in static)
    result.append(float(share))
    if not all(math.isfinite(v) for v in result):
        raise ValueError("non-finite guard feature")
    return result


def predict(bundle, x, static, share):
    """Evaluate an exported sklearn RandomForestRegressor without sklearn in pod."""
    f = features(x, static, share)
    if bundle.get("version") != 1 or bundle.get("feature_count") != len(f):
        raise ValueError("incompatible SLA guard bundle")
    trees = bundle.get("trees")
    if not trees:
        raise ValueError("SLA guard has no trees")
    total = 0.0
    for tree in trees:
        node = 0
        while True:
            feature, threshold, left, right, value = tree[node]
            if feature < 0:
                total += value
                break
            node = left if f[feature] <= threshold else right
    return max(0.0, min(1.0, total / len(trees)))


def decide(bundle, x, static, proposed, fallback, monitor_only=False):
    """Compare with the explicitly supplied xApp-SAFE share, never a default."""
    if proposed not in SHARES or fallback not in SHARES:
        raise ValueError("share outside frozen action set")
    if bundle.get("fallback_policy") != "const_sla" or bundle.get("fallback_share") != fallback:
        raise ValueError("runtime fallback differs from the fitted xApp-SAFE bundle")
    proposed_risk = predict(bundle, x, static, proposed)
    fallback_risk = predict(bundle, x, static, fallback)
    excess = max(0.0, proposed_risk - fallback_risk)
    if bundle.get("decision_rule") == "expected_utility":
        models = bundle.get("utility_models", {})
        if not isinstance(models, dict) or set(models) != {"urllc", "embb"}:
            raise ValueError("expected-utility guard needs both outcome forests")
        weights = bundle.get("utility_weights", {})
        if not isinstance(weights, dict) or set(weights) != {"urllc", "embb"}:
            raise ValueError("expected-utility guard needs declared utility weights")
        wu, we = float(weights["urllc"]), float(weights["embb"])
        if not all(math.isfinite(v) and v > 0 for v in (wu, we)):
            raise ValueError("invalid utility weights")
        pu, su = (predict_utility(models["urllc"], x, static, a, -4.6, 0.0)
                  for a in (proposed, fallback))
        pe, se = (predict_utility(models["embb"], x, static, a, -4.6, 1.0)
                  for a in (proposed, fallback))
        sla_benefit = wu * (su - pu)
        opportunity_cost = we * (pe - se)
        net_gain = sla_benefit - opportunity_cost
        alarm = net_gain > 0.0 and proposed != fallback
        blocked = alarm and not monitor_only
        return (fallback if blocked else proposed), {
            "proposed_share": proposed, "safe_share": fallback,
            "predicted_proposed_failure": proposed_risk,
            "predicted_safe_failure": fallback_risk,
            "excess_risk": excess,
            "predicted_proposed_urllc_utility": pu,
            "predicted_safe_urllc_utility": su,
            "predicted_proposed_embb_utility": pe,
            "predicted_safe_embb_utility": se,
            "sla_benefit": sla_benefit,
            "opportunity_cost": opportunity_cost,
            "net_switch_gain": net_gain,
            "decision_rule": "expected_utility",
            "alarm": alarm, "blocked": blocked,
            "monitor_only": bool(monitor_only),
        }
    cutoff = float(bundle["cutoff"])
    if not 0.0 <= cutoff <= 1.0 or not math.isfinite(cutoff):
        raise ValueError("invalid calibrated cutoff")
    alarm = excess > cutoff
    blocked = alarm and not monitor_only and proposed != fallback
    return (fallback if blocked else proposed), {
        "proposed_share": proposed,
        "safe_share": fallback,
        "predicted_proposed_failure": proposed_risk,
        "predicted_safe_failure": fallback_risk,
        "excess_risk": excess,
        "cutoff": cutoff,
        "alarm": alarm,
        "blocked": blocked,
        "monitor_only": bool(monitor_only),
    }


def predict_utility(bundle, x, static, share, low, high):
    """Evaluate a forest with the same wire format but signed utility leaves."""
    f = features(x, static, share)
    if bundle.get("version") != 1 or bundle.get("feature_count") != len(f):
        raise ValueError("incompatible utility forest")
    trees = bundle.get("trees")
    if not trees:
        raise ValueError("utility forest has no trees")
    total = 0.0
    for tree in trees:
        node = 0
        while True:
            feature, threshold, left, right, value = tree[node]
            if feature < 0:
                total += value
                break
            node = left if f[feature] <= threshold else right
    result = total / len(trees)
    if not math.isfinite(result):
        raise ValueError("non-finite predicted utility")
    return max(low, min(high, result))
