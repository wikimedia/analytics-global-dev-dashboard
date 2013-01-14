import sys, os
sys.path.insert(0, os.getcwd())
import logging
import argparse
logger = logging.getLogger(__name__)

import subprocess
subprocess.check_call(['/bin/tar', '-xf', 'pygeoip.tar'])

import pygeoip
gi = pygeoip.GeoIP('GeoIP.dat')

from squid import SquidRow

FIELDS = ['date', 'lang', 'project', 'site', 'country']

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--local', action='store_true', default=False, help='sets delim to be a space and ignores file name')
    args = parser.parse_args()
    return args 

def main():
    args = parse_args()
    for line in sys.stdin:
        try:
            toks = line.split()
            if args.local:
                filename = None
                squidline = ' '.join(toks)
            else:
                filename = toks[0]
                squidline = ' '.join(toks[1:])

            row = SquidRow(squidline)

            if args.local:
                provider = row['provider']
            else:
                provider = filename

            if row['old_init_request']:
                outline = '\t'.join(map(str, [provider] + map(row.__getitem__, FIELDS))) + '\n'
                sys.stdout.write(outline)
                sys.stdout.flush()
        except:
            logger.exception('encoutered exception on line:\n\t%s\n', line)

main()
