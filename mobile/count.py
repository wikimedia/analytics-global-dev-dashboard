#!/usr/bin/python
import argparse
import pprint
import datetime
import pandas as pd
import logging
from squid.mapreduce import count_files

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# output format
# 2013-04-01      ar      wikipedia.org   M       CI      orange-ivory-coast      4

def parse_args():
    parser = argparse.ArgumentParser('generate count file from all history '\
            'udp-filter zero logs')
    parser.add_argument('--glob',
            default='/a/squid/archive/zero/zero-*.log-201305*.gz',
            help='directory in which to find the zero log files')
    opts = vars(parser.parse_args())
    return opts

def main():
    opts = parse_args()
    criteria = (lambda r : r.site() in ['M', 'Z'], lambda r : r.old_init_request())
    fields = ('date', 'lang', 'project', 'site', 'country_code2', 'provider_from_fname')
    counts = count_files(opts['glob'],
            fields,
            criteria,
            count_event=10,
            fname='carrier.local.all.incremental', limit=10000)
    df = pd.DataFrame([v + (c,) for v,c in counts.items()],
            columns = ['date', 'lang', 'project', 'site', 'country', 'carrier', 'count'])
    df.date = df.date.apply(lambda d : datetime.datetime.strftime(d, '%Y-%m-%d'))
    logger.info('carriers: %s', pprint.pformat(df.carrier.unique()))
    df.to_csv('carrier.local.all', index=False, sep='\t')

if __name__ == '__main__':
    main()
