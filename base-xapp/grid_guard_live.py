"""Causal live inputs for the frozen grid predictor (one UE per slice).

The local-summary bridge is explicitly a testbed side channel, not E2.
No packet outcomes, traffic schedule, or future samples are inputs.
"""
import json
import math

from grid_outcome_guard import decide
from ncmab_live import greedy, predict
from grid_clean_prb import predict as predict_clean


def restricted_proposal(model, q):
    shares = model['shares']
    allowed = (20, 30, 35)
    if len(q) != len(shares) or not all(math.isfinite(v) for v in q):
        raise ValueError('invalid neural scores')
    indices = [shares.index(a) for a in allowed]
    return allowed[greedy([q[i] for i in indices])], shares[greedy(q)]


def ordinary_features(state, now, events, lag_events=None):
    """Reproduce offline complete-second binning and executed-action history."""
    end = math.floor(now)
    out = {}
    for sd, prefix in ((2, 'u'), (16777215, 'e')):
        entries = [e for e in state.ues.values() if e['sd'] == sd]
        if len(entries) != 1:
            raise ValueError('expected one UE per slice')
        rows = [r for r in entries[0]['rows'] if r['t'] <= now]
        if not rows or now - rows[-1]['t'] > 1.5:
            raise ValueError('stale E2 reports')
        bins = {}
        for r in rows:
            if not all(math.isfinite(v) for v in r.values()):
                raise ValueError('nonfinite E2 input')
            sec = math.floor(r['t'])
            if end-31 <= sec < end:
                bins.setdefault(sec, []).append(r)
        if set(bins) != set(range(end-31, end)):
            raise ValueError('incomplete E2 history')
        queue, arrival, tx = [], [], []
        for sec in range(end-30, end):
            b, prev = bins[sec], bins[sec-1]
            queue.append(sum(r['queue'] for r in b)/len(b))
            for field, target in (('arr', arrival), ('tx', tx)):
                delta = b[-1][field] - prev[-1][field]
                if delta < 0:
                    raise ValueError('E2 counter reset')
                target.append(delta*8/1e6)
        out.update({prefix+'_queue_last': queue[-1],
                    prefix+'_queue_delta': queue[-1]-queue[-10],
                    prefix+'_arrival_last': arrival[-1], prefix+'_tx_last': tx[-1]})
        # Ordinary least-squares slope on the same 30 completed seconds.
        out[prefix+'_queue_slope30'] = sum((i-14.5)*v for i,v in enumerate(queue))/2247.5
        for name, values in (('queue',queue),('arrival',arrival),('tx',tx)):
            for lag in range(1,11):
                out[prefix+'_'+name+'_lag'+str(lag)] = values[-lag]
        out[prefix+'_mcs_last'] = sum(r['mcs'] for r in bins[end-1])/len(bins[end-1])
    if not events or events[0][1] != 100 or events[0][0] > now-30:
        raise ValueError('missing observed neutral warmup')
    if any(t >= now for t, _ in events) or any(b[0] <= a[0] for a,b in zip(events, events[1:])):
        raise ValueError('invalid executed-action history')
    previous = events[-1][1]
    run_start = events[-1][0]
    for t, a in reversed(events[:-1]):
        if a != previous:
            break
        run_start = t
    low, last_low_end = 0., None
    for i, (t, a) in enumerate(events):
        until = events[i+1][0] if i+1 < len(events) else now
        if a == 20:
            low += max(0., until-max(t, now-30))
            last_low_end = until
    out.update(previous_share=previous, previous_share_seconds=min(30., now-run_start),
               low_share_seconds30=low,
               seconds_since_low_share=30. if last_low_end is None else min(30., now-last_low_end))
    # Lag identities refer to CNN decision boundaries, not intra-window commands.
    # Durations and queue-recovery features above use ALL actual commands.
    lag_events = events if lag_events is None else lag_events
    for lag in (2,3):
        out['action_lag'+str(lag)] = lag_events[-lag][1] if len(lag_events)-1>=lag else 100.
    return out


def bridge_features(path, now):
    with open(path) as f:
        bridge = json.load(f)
    if bridge.get('schema') != 'grid_local_summary_v1':
        raise ValueError('invalid local-summary bridge')
    if not 0 <= now-bridge['generated_at'] <= 2:
        raise ValueError('stale/future bridge timestamp')
    if not bridge['generated_at'] <= bridge['received_at'] <= now:
        raise ValueError('invalid bridge delivery timestamp')
    end = math.floor(now-.1)
    entry = bridge['entries'].get(str(end))
    if entry is None:
        raise ValueError('local-summary bridge has not delivered this cutoff')
    if entry['available_at_max']+.1 > now:
        raise ValueError('noncausal local summary')
    return entry['features'], dict(source='local_gNB_summary_bridge_not_E2',
        cutoff=end, generated_at=bridge['generated_at'], received_at=bridge['received_at'],
        available_at_max=entry['available_at_max'])


def validate_trigger(trigger, horizon):
    if trigger is None:
        return
    if (trigger.get('kind') != 'controlled_embb_arr_tx_replace'
            or not isinstance(trigger.get('pattern_mbps'), list)
            or len(trigger['pattern_mbps']) != 10
            or any(not isinstance(v, (int, float)) or not math.isfinite(v)
                   or not 0 <= v <= 100 for v in trigger['pattern_mbps'])
            or type(trigger.get('start_window')) is not int
            or type(trigger.get('stop_window')) is not int
            or not 0 <= trigger['start_window'] < trigger['stop_window'] <= horizon):
        raise ValueError('invalid controlled private KPM trigger')


def private_neural_input(neural_state, trigger, window):
    """Copy only the submitted xApp input; never change genuine guard telemetry."""
    active = trigger is not None and trigger['start_window'] <= window < trigger['stop_window']
    x, static = neural_state
    x, static = [list(row) for row in x], list(static)
    if active:
        if len(x) != 10:
            raise ValueError('trigger needs ten neural input samples')
        for row, v in zip(x, trigger['pattern_mbps']):
            row[4] = row[5] = v
        static[3] = sum(trigger['pattern_mbps'])/10.
    return (x, static), active


def evaluate(policy, state, now, events, window=0, lag_events=None):
    features = (ordinary_features(state, now, events) if lag_events is None else
                ordinary_features(state, now, events, lag_events=lag_events))
    local, provenance = bridge_features(policy['summary_path'], now)
    features.update(local)
    if policy['model'].get('schema') == 'grid_clean_cnn_v1':
        genuine_features=features.copy()
        untriggered, untriggered_outcomes = predict_clean(policy['model'],genuine_features)
        trigger=policy.get('private_trigger')
        active=bool(trigger and trigger['start_window']<=window<trigger['stop_window'])
        neural_features=genuine_features.copy()
        if active:
            for i,value in enumerate(trigger['pattern_mbps']):
                lag=str(10-i)
                neural_features['e_arrival_lag'+lag]=value
                neural_features['e_tx_lag'+lag]=value
        proposed, outcomes=(predict_clean(policy['model'],neural_features) if active
                            else (untriggered,untriggered_outcomes))
        selected, detail = decide(policy['grid_guard'],features,proposed,30)
        forced = window < policy.get('diagnostic_low_windows',0)
        guarded = selected
        if forced:
            selected = 20
        elif policy.get('guard_monitor_only'):
            selected = proposed
        info = dict(policy='ncmab_grid_guard',arm=[selected,0],
            branch='diagnostic_forced' if forced else ('monitor_only' if policy.get('guard_monitor_only')
                    else ('blocked' if selected!=proposed else 'accepted')),
            proposed_share=proposed, unrestricted_proposed_share=proposed,
            allowed_shares=[20,30,35],prb_outcomes=outcomes,guard=detail,
            effect_features={k:features[k] for k in policy['grid_guard']['features']},
            prb_features={k:neural_features[k] for k in policy['model']['features']},
            genuine_prb_features={k:genuine_features[k] for k in policy['model']['features']},
            untriggered_proposed_share=untriggered,
            untriggered_prb_outcomes=untriggered_outcomes,
            telemetry=provenance,decision_at=now,private_trigger_active=active,
            attack_input_source='controlled_private_KPM_state_override' if active else None,
            checkpoint_clean=not policy['model']['provenance']['poisoned'],
            diagnostic_forced=forced,guard_selected_share=guarded,
            decision_category=('forced_low_allocation_not_guarded' if forced else
                ('monitor_only_proposal_executed' if policy.get('guard_monitor_only') else
                ('override' if selected!=proposed else
                 ('agreement_with_SAFE' if proposed==30 else 'non_SAFE_accepted')))),
            unguarded_comparator=bool(policy.get('guard_monitor_only')))
        return policy['actions'][policy['model']['shares'].index(selected)],info
    neural_state = state.at(now, features['previous_share'], policy['lambda_ms'], policy['gbr_mbps'])
    if neural_state is None:
        raise ValueError('incomplete neural state')
    q_untriggered = predict(policy['model'], *neural_state)
    private_state, active = private_neural_input(neural_state, policy.get('private_trigger'), window)
    q = predict(policy['model'], *private_state) if active else q_untriggered
    untriggered, untriggered_full = restricted_proposal(policy['model'], q_untriggered)
    proposed, unrestricted = restricted_proposal(policy['model'], q)
    selected, detail = decide(policy['grid_guard'], features, proposed, 30)
    info = dict(policy='ncmab_grid_guard', arm=[selected, 0],
        branch='blocked' if selected != proposed else 'accepted',
        proposed_share=proposed, unrestricted_proposed_share=unrestricted,
        allowed_shares=[20,30,35], q=q, guard=detail,
        effect_features={k:features[k] for k in policy['grid_guard']['features']},
        telemetry=provenance, decision_at=now, private_trigger_active=active,
        untriggered_proposed_share=untriggered, untriggered_unrestricted_share=untriggered_full,
        untriggered_q=q_untriggered,
        attack_input_source='controlled_private_KPM_state_override' if active else None)
    return policy['actions'][policy['model']['shares'].index(selected)], info


def evaluate_held(policy, state, now, events, base, lag_events):
    """Recheck a HELD proposal with genuine fresh state; never run the CNN again.

    Outcome forecasts retain their trained ten-second horizon. A check is not
    a forecast of the remaining fraction of the nominal decision window.
    """
    features = ordinary_features(state, now, events, lag_events=lag_events)
    local, provenance = bridge_features(policy['summary_path'], now)
    features.update(local)
    proposed = base['proposed_share']
    selected, detail = decide(policy['grid_guard'], features, proposed, 30)
    info = dict(base, arm=[selected, 0], proposed_at=base['decision_at'],
                decision_at=now, guard=detail, guard_selected_share=selected,
                effect_features={k: features[k] for k in policy['grid_guard']['features']},
                telemetry=provenance, prediction_horizon_s=10,
                branch='blocked' if selected != proposed else 'accepted',
                decision_category='override' if selected != proposed else 'non_SAFE_accepted')
    return policy['actions'][policy['model']['shares'].index(selected)], info
