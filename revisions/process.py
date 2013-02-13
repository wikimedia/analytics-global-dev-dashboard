import argparse
from operator import itemgetter
import MySQLdb
from MySQLdb.cursors import SSCursor
import os, codecs, csv
from collections import defaultdict
import datetime
import pandas as pd

import gcat
import limnpy

cluster_mapping = {'enwiki':'s1',
                'bgwiki':'s2',
                'bgwiktionary':'s2',
                'cswiki':'s2',
                'enwikiquote':'s2',
                'enwiktionary':'s2',
                'eowiki':'s2',
                'fiwiki':'s2',
                'idwiki':'s2',
                'itwiki':'s2',
                'nlwiki':'s2',
                'nowiki':'s2',
                'plwiki':'s2',
                'ptwiki':'s2',
                'svwiki':'s2',
                'thwiki':'s2',
                'trwiki':'s2',
                'zhwiki':'s2',
                'commonswiki':'s4',
                'dewiki':'s5',
                'frwiki':'s6',
                'jawiki':'s6',
                'ruwiki':'s6',
                'eswiki':'s7',
                'huwiki':'s7',
                'hewiki':'s7',
                'ukwiki':'s7',
                'frwiktionary':'s7',
                'metawiki':'s7',
                'arwiki':'s7',
                'centralauth':'s7',
                'cawiki':'s7',
                'viwiki':'s7',
                'fawiki':'s7',
                'rowiki':'s7',
                'kowiki':'s7'
              }

def get_cur(lang):
    db = '%swiki' % lang
    cluster = cluster_mapping.get(db, 's3')
    host_name = '%s-analytics-slave.eqiad.wmnet' % cluster
    conn = MySQLdb.connect(host=host_name,
                           read_default_file=os.path.expanduser('~/.my.cnf'),
                           cursorclass=SSCursor,
                           db=db,
                           charset='utf8',
                           use_unicode=True)
    return conn.cursor() 

def get_rev(lang, start_date, cur):
    cache_name = '%s.revision.cache.csv' % lang

    if not os.path.exists(cache_name):
        batch_size = 100000
        query = """SELECT rev_len, rev_timestamp, rev_page, rev_user FROM revision WHERE rev_timestamp > %s"""
        print query % (start_date.strftime('%Y%m%d'),) 
        cur.execute(query, (start_date.strftime('%Y%m%d'),))
        outfile = codecs.open(cache_name, encoding='utf-8', mode='w')
        outcsv = csv.writer(outfile)
        so_far = 0
        while True:
            print 'processed %d lines' % so_far
            res = cur.fetchmany(batch_size)
            if not res:
                break
            outcsv.writerows(res)
            so_far += batch_size
        outfile.close()
        
    print 'loading revision cache from: %s' % cache_name
    df = pd.read_csv(cache_name, 
                       names=['rev_len', 'rev_timestamp', 'rev_page', 'rev_user'], 
                       encoding='utf-8')
    df = df.sort('rev_timestamp', inplace=True)
    df['cohort'] = 'Other'
    return df

def get_page(lang, cur):
    cache_name = '%s.page.cache.csv' % lang

    if not os.path.exists(cache_name):
        batch_size = 10000
        query = """SELECT page_id, page_namespace FROM page"""
        print query 
        cur.execute(query)
        outfile = codecs.open(cache_name, encoding='utf-8', mode='w')
        outcsv = csv.writer(outfile)
        while True:
            print 'processed %d lines' % batch_size
            res = cur.fetchmany(batch_size)
            if not res:
                break
            outcsv.writerows(res)
        outfile.close()
        
    print 'loading page cache from: %s' % cache_name
    df = pd.read_csv(cache_name, names=['page_id', 'page_namespace'], encoding='utf-8')
    return df

def get_bots(lang, cur):
    cache_name = '%s.bots.cache.csv' % lang
    if not os.path.exists(cache_name):
        batch_size = 10000
        query = """SELECT ug_user FROM user_groups WHERE ug_group = 'bot'"""
        print query 
        cur.execute(query)
        outfile = codecs.open(cache_name, encoding='utf-8', mode='w')
        outcsv = csv.writer(outfile)
        while True:
            print 'processed %d lines' % batch_size
            res = cur.fetchmany(batch_size)
            if not res:
                break
            outcsv.writerows(res)
        outfile.close()
        
    print 'loading page cache from: %s' % cache_name
    df = pd.read_csv(cache_name, names=['ug_user'], encoding='utf-8')
    return df



def filter_revs_ns(rev, page, ns):
    """filters revision by the namespaces given by ns"""
    merged = pd.merge(rev,page,left_on='rev_page', right_on='page_id')
    filtered = merged[merged['page_namespace'].isin(ns)]
    filtered = filtered[rev.columns]
    print 'filtered from %d to %d revs by restricting to ns: %s' % (len(rev), len(filtered), ns)
    return filtered

def tag_bots(rev, bots):
    """removes any revisions where user_id is a bot in the user_group table"""
    rev['cohort'][rev['rev_user'].isin(bots['ug_user'])] = 'bot'
    return rev

def get_size(revs):
    """computes the size of the wikipedia at each point in time.  has time complexity O(r) and
    space complexity O(r+t) (number of revisions and number of time bins).  Iterating over the 
    revisions in order it keeps an up to date hash of the current size of each article.  It 
    also keeps a hash of the current total size at each point in time.  Given a new revision
    it checks the old size of that article, and computes the delta wrt the new size.  Then it
    updates the total size hash for that revisions time bin by the delta.  Finally it overwrites
    the current size hash.  After all revisions have been processed, the size hash has the final
    size of each article and the totals hash has the deltas between time bins.  So to get the 
    actual total size at each point just take the cumulative sum of the totals hash"""

    sizes = defaultdict(int)
    deltas = defaultdict(lambda : defaultdict(int))

    for size, ts, page_id, user_id, cohort in revs.itertuples(index=False):
        old_size = sizes[page_id]
        delta = size - old_size # should be added to totals
        sizes[page_id] = size
        deltas[cohort][str(ts)[:len('YYYYMMDD')]] += delta

    date_parser = lambda s : datetime.datetime.strptime(s,'%Y%m%d')
    dfs = {} 
    for cohort, cohort_delta in deltas.items():
        df =  pd.DataFrame(cohort_delta.items(), columns=['date', 'delta'])

        df['date'] = df['date'].map(date_parser)
        df.sort('date', inplace=True)
        df['size'] = df['delta'].cumsum()
        dfs[cohort] = df
        print 'cohort: %s, df:\n%s' % (cohort,df)
    return dfs

def make_graphs(dfs, lang, basedir):
    # index on dates

    date_dfs = {}
    for cohort, df in dfs.items():
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        date_dfs[cohort] = df

    full_daily = pd.DataFrame({cohort : df['size'].asfreq('1D').fillna(method='ffill') for cohort, df in date_dfs.items()})
    full_daily['total'] = full_daily.sum(axis=1)

    # make daily size graph

    ds_daily = limnpy.DataSource('%s_bytes_daily' % lang, '%sWP Bytes Daily' % lang.upper(), full_daily)
    ds_daily.write(basedir)
    ds_daily.write_graph(basedir=basedir)

    # make monthly size graph
    full_monthly = full_daily.resample('1M')

    ds_monthly = limnpy.DataSource('%s_bytes_monthly' % lang, '%sWP Bytes Monthly' % lang.upper(), full_monthly)
    ds_monthly.write(basedir)
    ds_monthly.write_graph(basedir=basedir)

    # merge daily bytes added (deltas) and write csvs
    full_delta_daily = pd.DataFrame({cohort : df['delta'].asfreq('1D').fillna(0.0) for cohort, df in date_dfs.items()})
    for cohort, df in date_dfs.items():
        df.to_csv('orig_delta.%s.csv' % cohort)
    full_delta_daily['total'] = full_delta_daily.sum(axis=1)
    full_delta_daily.to_csv('full_delta_daily')
    
    # make daily bytes added (deltas) graph
    ds_delta_daily = limnpy.DataSource('%s_bytes_added_daily' % lang, '%sWP Bytes Added Daily' % lang.upper(), full_delta_daily)
    ds_delta_daily.write(basedir)
    ds_delta_daily.write_graph(basedir=basedir)
    
    # make monthly bytes added (deltas) graph
    delta_monthly = full_delta_daily.resample(rule='M', how='sum', label='right')
    ds_delta_monthly = limnpy.DataSource('%s_bytes_added_monthly' % lang, '%sWP Bytes Added Monthly' % lang.upper(), delta_monthly)
    ds_delta_monthly.write(basedir)
    ds_delta_monthly.write_graph(basedir=basedir)
    
def load_lang_file(f):
    return map(itemgetter(0),map(str.split, filter(lambda l: not l.startswith('#') and len(l) > 0, open(f).read().split('\n'))))

def parse_args():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-l', '--language', dest='languages', nargs=1, help='single language id (ie.e en, de, fr, ar)')
    group.add_argument('-f', '--langfile', dest='languages', type=load_lang_file, help='file containing list of langauge ids')
    parser.add_argument('-o', '--basedir', default='data', help='location for limn graphs')
    return vars(parser.parse_args())

def main():
    opts = parse_args()
    start_date = datetime.date(year=1999, month=1, day=1)
    for lang in opts['languages']:
        try:
            cur = get_cur(lang)

            rev = get_rev(lang, start_date, cur)
            bots = get_bots(lang, cur)
            page = get_page(lang, cur)

            rev = filter_revs_ns(rev, page, [0])
            rev = tag_bots(rev, bots)
            dfs = get_size(rev)
        
            make_graphs(dfs, lang, opts['basedir'])

        finally:
            print 'closing cursor'
            cur.close()

if __name__ == '__main__':
    main()
