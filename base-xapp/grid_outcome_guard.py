"""Standard-library inference for the frozen grid outcome predictor.

No telemetry collection or action transmission occurs here. Missing state and
unsupported actions raise errors: they must stop an experimental episode, not
silently change the controller or substitute another predictor.
"""
import math
import struct


def _float32(value):
    # sklearn trees compare float32 inputs against double thresholds.
    value = float(value)
    if not math.isfinite(value):
        raise ValueError('nonfinite feature')
    try:
        result = struct.unpack('f', struct.pack('f', value))[0]
    except (OverflowError, struct.error) as exc:
        raise ValueError('feature outside float32 range') from exc
    if not math.isfinite(result):
        raise ValueError('feature outside float32 range')
    return result


def _tree(tree, features):
    node = 0
    for _ in range(len(tree)):
        if not isinstance(node, int) or not 0 <= node < len(tree):
            raise ValueError('invalid tree node')
        feature, threshold, left, right, value = tree[node]
        if feature == -2:
            if not math.isfinite(value):
                raise ValueError('nonfinite leaf')
            return value
        if not isinstance(feature, int) or not 0 <= feature < len(features):
            raise ValueError('invalid tree feature')
        if not math.isfinite(threshold):
            raise ValueError('nonfinite threshold')
        node = left if features[feature] <= threshold else right
    raise ValueError('cyclic tree')


def predict(bundle, snapshot, share):
    if bundle.get('schema') != 'grid_outcome_guard_v1':
        raise ValueError('incompatible grid guard')
    if share not in bundle['shares']:
        raise ValueError('unsupported action: %s' % share)
    missing = set(bundle['features'])-set(snapshot)
    if missing:
        raise ValueError('missing features: '+', '.join(sorted(missing)))
    features = [_float32(snapshot[key]) for key in bundle['features']]
    trees = bundle['models'][str(share)]
    if len(trees) != 2:
        raise ValueError('expected two outcome models')
    return (min(1., max(0., _tree(trees[0], features))),
            max(0., _tree(trees[1], features)))


def decide(bundle, snapshot, proposed, safe):
    """Same one-window SLA-priority rule as the offline audit; no traffic model."""
    if safe != bundle['safe_share']:
        raise ValueError('SAFE differs from frozen policy')
    budget = float(bundle['failure_budget'])
    if not math.isfinite(budget) or not 0 < budget < 1:
        raise ValueError('invalid failure budget')
    pr, pe = predict(bundle, snapshot, proposed)
    sr, se = predict(bundle, snapshot, safe)
    switch = proposed != safe and (
        (pr > budget and sr <= budget)
        or (pr <= budget and sr <= budget and se > pe)
        or (pr > budget and sr > budget and (sr < pr or (sr == pr and se > pe))))
    previous = snapshot['previous_share']
    support = bundle['training_transition_counts'].get('%g->%g' % (previous, safe), 0)
    return (safe if switch else proposed), dict(
        proposed_share=proposed, safe_share=safe, switched=bool(switch),
        predicted_proposal_failure=pr, predicted_safe_failure=sr,
        predicted_proposal_embb_mbps=pe, predicted_safe_embb_mbps=se,
        safe_transition_training_n=support,
        rule='one_window_SLA_priority', counterfactual_outcomes_measured=False)
