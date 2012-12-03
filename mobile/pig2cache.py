import sys
import os
import operator
from collections import OrderedDict

max_outf = 1024
out_files = OrderedDict()
in_file = open(sys.argv[1])

out_dir  = sys.argv[2]
if not os.path.exists(out_dir):
    os.mkdir(out_dir)


reorderer = operator.itemgetter(5,0,1,2,3,4)

for line in in_file:
    try:
        toks = line.strip().split(',')
        orig_path = toks[0]
        reordered = reorderer(toks[1:]) + ('',)
        out_line = ','.join(reordered)
        # hdfs://analytics1010.eqiad.wmnet/traffic/zero/digi-malaysia.log-20120405.gz
        orig_f = os.path.split(orig_path)[1]
        if orig_f not in out_files:
            if len(out_files) >= 999:
                # close LRU
                out_files.values()[0].close()
                del out_files[out_files.keys()[0]]
            out_f = open(os.path.join(out_dir, orig_f + '.gz.counts'), 'w')
            print 'len(out_files): %d' % len(out_files)
            out_files[orig_f] = out_f
        else:
            out_f = out_files[orig_f]
        out_f.write(out_line + '\n')
    except:
        print 'exception encountered on line: %s' % line
        raise

for out_file in out_files.values():
    out_file.close()
