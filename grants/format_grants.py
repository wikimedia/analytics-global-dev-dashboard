import logging
import argparse
from pandas import DataFrame
import pandas as pd
import itertools
import xlrd
import datetime
import re

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
    keep_cols = set(['Amount Funded in USD',
                 'Amount Requested in USD',
                 'By UN region',
                 'Catalyst Program',
                 'By country (short form)',
                 'UN developing country classification',
                 'Human Development Index 2011 Category',
                 'Grant Status',
                 'Grant stage',
                 'Date opened',
                 'Date of decision']) & set(rows.columns)
    clean = rows[list(keep_cols)]
    return clean


def write_limn_files(pt, val_key=None, group_key=None, limn_id=None, limn_name=None):
    pt['date'] = map(pd.Timestamp.to_datetime, pt.index)
    limn_rows = [dict(pt.irow(i)) for i in range(len(pt))]
    #logging.debug('limn_rows: %s', limn_rows)
    limn_labels = list(pt.columns)

    if not (limn_id or limn_name):
        limn_name = ('Grants %s by %s' % (val_key, group_key))
        limn_id = limn_name.replace(' ', '_').lower()
        limn_id = re.sub('\W', '', limn_id)
    limnpy.writedicts(limn_id, limn_name, limn_rows)


def write_groups(all_rows):
    group_keys = set(['By UN region',
                  'Catalyst Program',
                  'By country (short form)',
                  'UN developing country classification',
                  'Human Development Index 2011 Category',
                  'Grant Status',
                  'Grant stage']) & set(all_rows.columns)
    val_date_keys = [('Amount Funded in USD', 'Date of decision'),
                ('Amount Requested in USD', 'Date opened')]
                 
    for group_key, (val_key, date_key) in itertools.product(group_keys, val_date_keys):
        logging.debug('grouping by (%s, %s), summed %s', group_key, date_key, val_key)
        pt = all_rows.pivot_table(values=val_key, rows=date_key, cols=group_key, aggfunc=sum)
        pt = pt.fillna(0)
        pt_cum = pt.cumsum()

        write_limn_files(pt, val_key, group_key)
        write_limn_files(pt_cum, 'Cumulative ' + val_key, group_key)


def write_total(all_rows):
    req_pt_all = all_rows.pivot_table(values='Amount Requested in USD', rows='Date opened', aggfunc=sum)
    req_pt_all = req_pt_all.fillna(0)
    req_pt_all_cum = req_pt_all.cumsum()
    write_limn_files(DataFrame(req_pt_all),
                     limn_id='grants_amount_requested_in_usd_all',
                     limn_name='Grants Amount Requested In USD All')
    write_limn_files(DataFrame(req_pt_all_cum), 
                     limn_id='grants_cumulative_amount_requested_in_usd_all',
                     limn_name='Grants Cumulative Amount Requested In USD All')
    
    funded_pt_all = all_rows.pivot_table(values='Amount Funded in USD', rows='Date opened', aggfunc=sum)
    funded_pt_all = funded_pt_all.fillna(0)
    funded_pt_all_cum = funded_pt_all.cumsum()
    write_limn_files(DataFrame(funded_pt_all),
                     limn_id='grants_amount_funded_in_usd_all', 
                     limn_name='Grants Amount Funded In USD All')
    write_limn_files(DataFrame(funded_pt_all_cum), 
                     limn_id='grants_cumulative_amount_funded_in_usd_all', 
                     limn_name='Grants Cumulative Amount Funded In USD All')


def main():
    opts = parse_args()

    f = gcat.get_file(opts['grants_file'], fmt='pandas', usecache=True)

    all_rows = pd.DataFrame()
    for sn in ['%d-%d' % (y-1, y) for y in opts['years']]:
        logger.debug('processing fiscal year: %s', sn)
        rows = f[sn]
        all_rows = pd.concat([all_rows, clean_rows(rows)])
        
        write_groups(all_rows)
        write_total(all_rows)
    
    logger.debug('len(all_rows) : %s, type(all_rows): %s, all_rows: %s', len(all_rows), type(all_rows), all_rows)

if __name__ == '__main__':
    main()
