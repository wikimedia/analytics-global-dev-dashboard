import logging
import argparse
from pandas import DataFrame
import pandas as pd
import itertools
import xlrd
import datetime
import re
import pprint
import sys

import limnpy
import gcat


root_logger = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s]\t[%(threadName)s]\t[%(funcName)s:%(lineno)d]\t%(message)s')
ch.setFormatter(formatter)
root_logger.addHandler(ch)
root_logger.setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)



basedir = 'data'

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
    logger.debug('pt:\n%s', pt)

    if not (limn_id or limn_name):
        limn_name = ('Grants %s by %s' % (val_key, group_key))
        limn_id = limn_name.replace(' ', '_').lower()
        limn_id = re.sub('\W', '', limn_id)
    ds = limnpy.DataSource(limn_id, limn_name, pt)
    try:
        ds.write(basedir)
        g = ds.get_graph()
        g.__graph__['options']['stackedGraph'] = True
        g.write(basedir=basedir)
        return g
    except:
        logger.debug('error on limn_name: %s', limn_name)
        logger.debug('ds.__source__:\n %s', pprint.pformat(ds.__source__))
        logger.debug('type(ds.__data__):%s', type(ds.__data__))
        logger.debug('ds.__data__.columns: %s', ds.__data__.columns)
        logger.debug('ds.__data__.index: %s', ds.__data__.index)
        raise


def write_groups(all_rows):
    group_keys = set(['By UN region',
                  'Catalyst Program',
                  'Country of impact (short form)',
                  'Global South (impact)',
                  'Global South (requestor)',
                  'By UN region (impact)',
                  'Grant Status',
                  'Grant stage']) & set(all_rows.columns)
    val_date_keys = [('Amount Funded in USD', 'Date of decision'),
                ('Amount Requested in USD', 'Date opened')]

    graphs = []
    for group_key, (val_key, date_key) in itertools.product(group_keys, val_date_keys):
        logger.debug('grouping by (%s, %s), summed %s', group_key, date_key, val_key)
        pt = all_rows.pivot_table(values=val_key, rows=date_key, cols=group_key, aggfunc=sum)
        pt = pt.fillna(0)
        pt_cum = pt.cumsum()

        g = write_limn_files(pt, val_key, group_key)
        g_cum = write_limn_files(pt_cum, 'Cumulative ' + val_key, group_key)
        graphs.append(g)
        graphs.append(g_cum)
    return graphs


def write_total(all_rows):
    # req_pt_all = all_rows.pivot_table(values='Amount Requested in USD', rows='Date opened', aggfunc=sum)
    req_pt_all = pd.DataFrame(all_rows.groupby('Date opened').sum()['Amount Requested in USD'])
    req_pt_all.columns = ['Amount Requested in USD']
    logger.debug('req_pt_all:\n%s', req_pt_all)
    req_pt_all = req_pt_all.fillna(0)
    req_pt_all_cum = req_pt_all.cumsum()
    write_limn_files(DataFrame(req_pt_all),
                     limn_id='grants_amount_requested_in_usd_all',
                     limn_name='Grants Amount Requested In USD All')
    write_limn_files(DataFrame(req_pt_all_cum), 
                     limn_id='grants_cumulative_amount_requested_in_usd_all',
                     limn_name='Grants Cumulative Amount Requested In USD All')
    
    funded_pt_all = pd.DataFrame(all_rows.groupby('Date of decision').sum()['Amount Funded in USD'])
    funded_pt_all.columns = ['Amount Funded in USD']
    funded_pt_all = funded_pt_all.fillna(0)
    funded_pt_all_cum = funded_pt_all.cumsum()
    g = write_limn_files(DataFrame(funded_pt_all),
                         limn_id='grants_amount_funded_in_usd_all', 
                         limn_name='Grants Amount Funded In USD All')
    g_cum = write_limn_files(DataFrame(funded_pt_all_cum), 
                              limn_id='grants_cumulative_amount_funded_in_usd_all', 
                              limn_name='Grants Cumulative Amount Funded In USD All')
    return (g, g_cum)


def add_global_south(rows):
    meta = gcat.get_file('Global South and Region Classifications', fmt='pandas', sheet='MaxMind Countries Final')
    logger.debug('meta:\n%s', meta)
    labels = dict(meta[['Country', 'global south']].values)

    req_gs = rows['Country of requestor (short form)'].apply(lambda c : labels.get(c,'Unkown Country Name'))
    rows['Global South (requestor)'] = req_gs
    logger.debug('req_gs:\n%s', req_gs)
    impact_gs = rows['Country of impact (short form)'].apply(lambda c : labels.get(c,'Unkown Country Name'))
    rows['Global South (impact)'] = impact_gs
    logger.debug('impact_gs:\n%s', impact_gs)
    return rows


def main():
    opts = parse_args()

    f = gcat.get_file(opts['grants_file'], fmt='pandas_excel', usecache=False)

    all_rows = pd.DataFrame()
    graphs = []
    for sn in ['FY%d%d' % (y-1, y) for y in opts['years']]:
        logger.debug('processing fiscal year: %s', sn)
        rows = f.parse(sn, skiprows=2)
        logger.debug(rows)
        # all_rows = pd.concat([all_rows, clean_rows(rows)])
        all_rows = pd.concat([all_rows, rows])
        all_rows = add_global_south(all_rows)
        
        graphs.extend(write_groups(all_rows))
        graphs.extend(write_total(all_rows))
    
    db = limnpy.Dashboard('grants', 'Wikimedia Grants', 'Dashboard')
    db.add_tab('all', map(lambda g : g.__graph__['id'], graphs))
    db.write(basedir)

if __name__ == '__main__':
    main()
