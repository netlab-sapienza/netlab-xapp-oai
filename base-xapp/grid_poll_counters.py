"""Counter semantics shared by the single-DRB polling runtime and auditor.

The OAI RLC TX statistic is uint32; its protobuf field is uint64. Only
boundary-local rollovers are recoverable under this experimental contract.
This is not support for summed multi-bearer counters or arbitrary resets.
"""
import math

TX_MODULUS = 2**32


def tx_increment(previous, current, interval_s):
    if any(type(v) is not int or not 0<=v<TX_MODULUS for v in (previous,current)):
        raise ValueError('expected single-bearer uint32 E2 TX counter')
    delta=current-previous
    wrapped=delta<0
    if wrapped:
        if not (previous>.9*TX_MODULUS and current<.1*TX_MODULUS
                and math.isfinite(interval_s) and 0<interval_s<=1.5):
            raise ValueError('E2 TX counter reset or ambiguous rollover')
        delta+=TX_MODULUS
    return delta,wrapped
