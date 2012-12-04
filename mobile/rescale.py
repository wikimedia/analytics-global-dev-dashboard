import pandas as pd
import sys

scale = float(sys.argv[1])

t = pd.read_table(sys.argv[2], header=None, sep=',')
print t
t['X6'] = t['X6'] * scale
t.to_csv(sys.argv[2] + '.rescaled_%.1f' % scale, index=False, header=False)
