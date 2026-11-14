"""Stdlib inference for the frozen, clean, three-action outcome CNN.

Not the historical scalar-Q ensemble. No online training or input trigger.
"""
import math
import struct

CHANNELS = [s+'_'+k for s in ('u', 'e') for k in ('queue', 'arrival', 'tx')]
FEATURES = [c+'_lag'+str(i) for i in range(10, 0, -1) for c in CHANNELS] + [
    'u_mcs_last', 'e_mcs_last', 'previous_share', 'previous_share_seconds',
    'action_lag2', 'action_lag3', 'low_share_seconds30', 'seconds_since_low_share']


def validate(model):
    if (model.get('schema') != 'grid_clean_cnn_v1' or model.get('shares') != [20,30,35]
            or model.get('features') != FEATURES or model.get('failure_budget') != .01
            or type(model.get('provenance', {}).get('poisoned')) is not bool
            or model.get('failure_link','linear') not in ('linear','sigmoid')):
        raise ValueError('invalid clean CNN contract')
    provenance=model['provenance']
    if provenance['poisoned']:
        attack=provenance.get('attack',{})
        motif=attack.get('motif_mbps',[])
        if (attack.get('kind')!='temporal_embb_arrival_tx_motif_v1'
                or attack.get('target_share')!=20 or not isinstance(motif,list)
                or len(motif)!=10 or any(not isinstance(v,(int,float)) or not math.isfinite(v)
                                       or not 0<=v<=100 for v in motif)):
            raise ValueError('invalid weight-poisoned CNN provenance')
    w = model['width']
    shapes = {'conv.0.weight': (w,6,3), 'conv.0.bias': (w,),
              'conv.2.weight': (w,w,3), 'conv.2.bias': (w,),
              'head.0.weight': (16,2*w+8), 'head.0.bias': (16,),
              'head.2.weight': (6,16), 'head.2.bias': (6,)}
    def check(value, shape):
        if not shape:
            if not isinstance(value,(int,float)) or not math.isfinite(value):
                raise ValueError('nonfinite CNN parameter')
        else:
            if not isinstance(value,list) or len(value)!=shape[0]:
                raise ValueError('CNN parameter shape mismatch')
            for v in value:
                check(v,shape[1:])
    for key, shape in shapes.items():
        check(model['state'][key],shape)
    for name,n in [('mean',68),('scale',68),('y_mean',2),('y_scale',2)]:
        check(model[name],(n,))
    if any(v<=0 for name in ('scale','y_scale') for v in model[name]):
        raise ValueError('invalid CNN scales')


def predict(model, features):
    x = [float(features[k]) for k in FEATURES]
    if not all(math.isfinite(v) for v in x) or min(x[:60])<0:
        raise ValueError('invalid clean CNN inputs')
    x[:60] = [math.log1p(v) for v in x[:60]]
    x = [struct.unpack('f',struct.pack('f',(v-m)/s))[0]
         for v,m,s in zip(x,model['mean'],model['scale'])]
    h = [[x[t*6+c] for t in range(10)] for c in range(6)]
    params = model['state']
    for name in ('conv.0','conv.2'):
        h = [[max(0.,bias+sum(w[c][k]*h[c][t+k]
                for c in range(len(h)) for k in range(3)))
              for t in range(len(h[0])-2)]
             for w,bias in zip(params[name+'.weight'],params[name+'.bias'])]
    h = [max(row) for row in h]+[sum(row)/len(row) for row in h]+x[60:]
    for name in ('head.0','head.2'):
        h = [bias+sum(w*v for w,v in zip(row,h))
             for row,bias in zip(params[name+'.weight'],params[name+'.bias'])]
        if name=='head.0':
            h = [max(0.,v) for v in h]
    h = [v*model['y_scale'][i%2]+model['y_mean'][i%2] for i,v in enumerate(h)]
    if model.get('failure_link','linear') == 'sigmoid':
        for i in range(3):
            value=h[2*i]
            h[2*i]=(1/(1+math.exp(-value)) if value>=0 else
                     math.exp(value)/(1+math.exp(value)))
    outcomes = [[min(1.,max(0.,h[2*i])),max(0.,h[2*i+1])] for i in range(3)]
    feasible = [i for i,row in enumerate(outcomes) if row[0]<=.01]
    if feasible:
        best = max(feasible,key=lambda i:(outcomes[i][1],model['shares'][i]))
    else:
        best = max(range(3),key=lambda i:(-outcomes[i][0],outcomes[i][1],model['shares'][i]))
    return model['shares'][best], outcomes
