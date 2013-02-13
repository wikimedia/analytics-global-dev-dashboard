import sys
import os
import operator
from collections import OrderedDict
import re

max_outf = 1024
out_files = OrderedDict()
in_file = open(sys.argv[1])

out_dir  = sys.argv[2]
if os.path.exists(out_dir):
    print 'exiting becuase out directory already exists'
    sys.exit()
else:
    os.mkdir(out_dir)


reorderer = operator.itemgetter(5,0,1,2,3,4)

for line in in_file:
    try:
        toks = line.strip().split(',')
        orig_f = toks[0]

        orig_f = orig_f.replace('tab.', '')
        m_with_zero = re.match('zero-(.*)\.log-\d{8}\.gz', orig_f)
        m_without_zero = re.match('(.*)\.log-\d{8}\.gz', orig_f)
        sampled = re.match('sampled-1000\.log-\d{8}\.gz', orig_f)
        if m_with_zero:
            provider = m_with_zero.groups(1)[0]
        elif m_without_zero and not sampled:
            provider = m_without_zero.groups(1)[0]
        else:
            provider = ''

        reordered = list(reorderer(toks[1:]))
        reordered = reordered + [provider]
        out_line = ','.join(reordered)

        if orig_f not in out_files:
            if len(out_files) >= 999:
                # close LRU
                name, f = out_files.popitem(last=False)
                f.close()
            if orig_f.endswith('.gz'):
                out_fname = os.path.join(out_dir, orig_f + '.counts')
            else:
                out_fname = os.path.join(out_dir, orig_f + '.gz.counts')

            out_f = open(out_fname, 'a')
            # print 'len(out_files): %d' % len(out_files)
            out_files[orig_f] = out_f
        else:
            out_f = out_files[orig_f]

        out_f.write(out_line + '\n')
        out_f.flush()
    except:
        print 'exception encountered on line: %s' % line
        raise

for out_file in out_files.values():
    out_file.close()
