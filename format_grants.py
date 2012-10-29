import logging
import argparse
from pandas import DataFrame
import pandas as pd
import itertools
import xlrd
import datetime
from functional import compose
from operator import itemgetter, attrgetter
import limnpy
import gcat

root_logger = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s]\t[%(threadName)s]\t[%(funcName)s:%(lineno)d]\t%(message)s')
ch.setFormatter(formatter)
root_logger.addHandler(ch)
root_logger.setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser('Utility for creating limn charts from WMF Grants spreadsheets',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--grants_file',
                        default='Wikimedia Foundation Grants',
                        help='the Google Drive document in which to find the relevant sheets')
    parser.add_argument('-y', '--years',
                        nargs='+',
                        type=int,
                        default=[2013],
                        help='The end of the fiscal year for which to gather grants')
    args = parser.parse_args()
    return vars(args)

def clean_rows(rows):
    keep_cols = ['Amount Funded in USD',
                 'Amount Requested in USD',
                 'By UN region',
                 'Catalyst Program',
                 'By country (short form)',
                 'UN developing country classification',
                 'Human Development Index 2011 Category',
                 'Grant Status',
                 'Grant stage',
                 'Date opened',
                 'Date of decision',
                 ]
    clean = rows[keep_cols]
    return clean


def limnify(grouped):
    for rowx in range(len(grouped)):
        logging.debug('row: %s', grouped.ix[rowx])

def main():
    opts = parse_args()

    f = gcat.get_file(opts['grants_file'], fmt='pandas', usecache=True)

    all_rows = pd.DataFrame()
    for sn in ['%d-%d' % (y-1, y) for y in opts['years']]:
        logger.debug('processing fiscal year: %s', sn)
        rows = f[sn]
        all_rows = pd.concat([all_rows, clean_rows(rows)])

    group_keys = ['By UN region',
                  'Catalyst Program',
                  'By country (short form)',
                  'UN developing country classification',
                  'Human Development Index 2011 Category',
                  'Grant Status',
                  'Grant stage']
    val_date_keys = [('Amount Funded in USD', 'Date of decision'),
                ('Amount Requested in USD', 'Date opened')]
                 
    for group_key, (val_key, date_key) in itertools.product(group_keys, val_date_keys):
        logging.debug('grouping by (%s, %s), summed %s', group_key, date_key, val_key)
        grouped = all_rows.groupby([group_key, date_key]).sum()
        logging.debug('grouped:\n%s', grouped)
        limnify(grouped)

    logger.debug('len(all_rows) : %s, type(all_rows): %s, all_rows: %s', len(all_rows), type(all_rows), all_rows)

if __name__ == '__main__':
    main()
