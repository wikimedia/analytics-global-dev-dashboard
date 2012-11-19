"""
this allows the script to work before it has been installed
and in the case where it is installed, because the entry is
inserted, python still uses the local copy of sqproc
"""
import sys, os
sys.path.insert(0, os.path.abspath('../..'))

from sqproc import ProcessManager, SquidArgumentParser, SquidProcessor, SquidRow
import argparse
import logging
import pprint
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import csv
import re
import cProfile
from collections import defaultdict, Counter
from operator import itemgetter
from nesting import Nest
import simplejson as json
import itertools
import pandas as pd
import os.path

import limnpy, gcat

DEFAULT_PROVIDERS = ['zero-digi-malaysia',
                     'zero-grameenphone-bangladesh',
                     'zero-orange-kenya',
                     'zero-orange-niger',
                     'zero-orange-tunesia',
                     'zero-orange-uganda',
                     'zero-orange-cameroon',
                     'zero-orange-ivory-coast',
                     'zero-telenor-montenegro',
                     'zero-tata-india',
                     'zero-dtac-thailand',
                     'zero-saudi-telecom']


class TrafficCounter(SquidProcessor):
    """
    This class tallies the number of initial page views for each langauge for each project for each day
    """

    def __init__(self, provider, args):
        super(TrafficCounter, self).__init__()
        self.provider = provider
        self.args = args
        self.count_event = 10
        self.rows = []

    def __str__(self): 
        return 'mobile_edits'


    def __repr__(self):
        return self.__dict__()


    def process_line(self, line, filename):
        """
        this function is the main entry point for all of the code in this module.  It is the iterface
        through which the ProcessManager gives this processor a raw string line from the squid files
        """
        #logging.debug('processing line: %s' % (line))
        try:
            row = SquidRow(line)
        except ValueError:
            logging.debug('malformed line found in file: %s' % filename)
            #logging.info('exiting due to malformed line')
            return

        # if row['site'] == 'M':
        #     print 'found Mobile row!'
        #     print 'mime_type: %s' % ['mime_type']
        #     print 'status_code: %s' % row['status_code']
        #     print 'url_path: %s' % row['url_path']

        if not row['init_request']:
            return

        if not row['project'] == 'wikipedia':
            return

        try:
            dt = row['datetime']
        except ValueError:
            #logging.info('exiting due to malformed timestamp')
            return

        if dt < self.args.start_date or dt > self.args.end_date:
            #logging.info('exiting due to invalid date: %s, start: %s, end: %s', dt, self.args.start_date, self.args.end_date)
            return

        # copy relevant fields for any rows we want to keep
        keepers = ['datetime', 'site'] #, 'action']
        keep_row = {}
        for keeper_key in keepers:
            keep_row[keeper_key] = row[keeper_key]
        self.rows.append(keep_row)


    def finalize(self):
        logging.info('processed %d lines, len(self.rows)=%d' % (self.__num_lines__, len(self.rows)))

        for row in self.rows:
            row['date'] = row['datetime'].date()
        keys = ['date', 'site']
        values = map(itemgetter(*keys), self.rows)
        counts = defaultdict(int)
        for tup in values:
            counts[tup] += self.count_event
        count_tuples = map(lambda (key, count): key + (count,), counts.items())
        count_tuples.sort(key=itemgetter(*range(len(keys))))
        grouped = itertools.groupby(count_tuples, key=itemgetter(0))
        limn_rows = []
        for date, date_rows in grouped:
            site_count = {}
            for row in date_rows:
                print 'date_row: %s' % str(row)
                site_count[row[1]] = row[2]
            limn_row = [date, site_count.get('X',0), site_count.get('M', 0),  site_count.get('Z', 0)]
            limn_rows.append(limn_row)
        provider_name = '_'.join(self.provider.split('-'))
        limn_id = '%s_partner_report' % provider_name
        partner_name = self.provider.replace('-', ' ').title()
        limn_name = '%s Partner Report' % partner_name
        source = limnpy.write(limn_id, limn_name, ['date', 'Main (X)', 'Mobile (M)', 'Zero (Z)'], limn_rows)
        limnpy.writegraph(limn_id, limn_name, [source])
        return source


def parse_args():
    parser = SquidArgumentParser(description='Process a collection of squid logs and write certain extracted metrics to file')
    parser.add_argument('providers', 
                        metavar='PROVIDER_IDENTIFIER',
                        nargs='*',
                        default=DEFAULT_PROVIDERS,
                        help='list of provider identifiers used in squid log file names')
    parser.add_argument('--name_format',
                        dest='name_format',
                        type=str,
                        default='%s.log-%.gz',
                        help='a printf style format string which is formatted with the tuple: (provider_name, date_representation')
    parser.set_defaults(datadir='/a/squid/archive/zero')


    args = parser.parse_args()
    # custom logic for which files to grab
    prov_files = {}
    for prov in args.providers:
        args.basename = prov
        logging.info('args prior to ge_files: %s', pprint.pformat(args.__dict__))
        prov_files[prov] = SquidArgumentParser.get_files(args)
    setattr(args, 'squid_files', prov_files)

    
    logging.info(pprint.pformat(args.__dict__))
    return args


def write_total(datasources):
    vm = gcat.get_file('WP Zero Partner - Versions', fmt='pandas', usecache=True)
    vm = vm.set_index(vm['Partner Identifier'])
    names = {'M' : 'Mobile (M)', 'Z' : 'Zero (Z)', 'X' : 'Main (X)'}
    logging.info('type(vm.ix[1,\'Start Date\']: %s', type(vm.ix[1,'Start Date']))

    dfs = []
    for pid, datasource in datasources.items():
        url = datasource['url']
        logging.info('url: %s', url)
        path = os.path.join('datafiles', os.path.split(url)[1])
        logging.debug('using datasource at: %s', path)
        df = pd.read_csv(path, parse_dates=[0], date_parser=lambda dstr : datetime.datetime.strptime(dstr, limnpy.limn_date_fmt))
        df = df.set_index(df['date'])
        start_date = vm.ix[pid, 'Start Date']
        if not start_date:
            continue
        valid_df = df.ix[df.index > start_date]

        free_versions = set(map(names.get, map(unicode.strip, vm.ix[pid, 'Version'].split(','))))
        ignore_versions = list(set(names.values()) - free_versions)
        valid_df[ignore_versions] = 0
        dfs.append(valid_df)
        
    long_fmt = pd.concat(dfs)
    final = long_fmt.groupby('date').sum()
    final.index = final.index.map(lambda ts : ts.to_pydatetime())
    cols = list(final.reset_index().columns)
    rows = list(final.itertuples())
    limnpy.write('mobile_traffic_by_version', 'Mobile Traffic by Version', cols, rows)



def main():
    args = parse_args()

    datasources = {}
    for prov in args.providers:
        processors = [TrafficCounter(prov, args)]
        manager = ProcessManager(args.squid_files[prov], processors, options=args)
        res = manager.process_files_par()
        datasources[prov] = res[0] # should only be as many elements in res as there are processors (i.e. 1)
    write_total(datasources)

if __name__ == '__main__':
    #main()
    cProfile.run('main()', '/home/erosen/src/sqproc/sqproc/examples/zero_reports/zero-reports.prof')
