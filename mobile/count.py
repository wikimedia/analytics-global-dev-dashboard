#!/usr/bin/python 
"""
logging set up
"""
import logging
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s]\t[%(name)s]\t[%(processName)s]\t[%(funcName)s:%(lineno)d]\t%(message)s')
ch.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(ch)

logger = logging.getLogger(__name__)
logger.debug('got logger for count.py with name: %s', __name__)


import sys, os, argparse
import datetime, dateutil
import gzip
import subprocess, multiprocessing
import pprint
import re
import pandas as pd
import StringIO
import itertools
from collections import defaultdict
import copy

import sqproc
import gcat
import limnpy

DEFAULT_PROVIDERS = ['digi-malaysia',
                     'grameenphone-bangladesh',
                     'orange-kenya',
                     'orange-niger',
                     'orange-tunesia',
                     'orange-uganda',
                     'orange-cameroon',
                     'orange-ivory-coast',
                     'telenor-montenegro',
                     'tata-india',
                     'saudi-telecom',
                     'dtac-thailand']

LEVELS = {'DEBUG': logging.DEBUG,
          'INFO': logging.INFO,
          'WARNING': logging.WARNING,
          'ERROR': logging.ERROR,
          'CRITICAL': logging.CRITICAL}


fields = ['date', 'lang', 'project', 'site', 'country', 'provider']


def count_file((fname, cache_dir, filter_by_mobile)):
    logger.debug('processing file: %s', fname)
    try:
        fin = gzip.GzipFile(fname)
        fout_name = fname.split('/')[-1] + '.out'
        fout = open(fout_name, 'w')

        for i, line in enumerate(fin):
            try:
                row = sqproc.SquidRow(line)
            except ValueError:
                continue

            if filter_by_mobile and not row['old_init_request']:
                continue

            try:
                dt = row['datetime'].date()
            except ValueError:
                continue

            out_line = '\t'.join(map(str, map(row.__getitem__, fields)))
            fout.write(out_line + '\n')

        fout.close()
        count_cmd = 'cat %s | sort | uniq -c' % fout_name
        raw_counts = subprocess.check_output(count_cmd, shell=True)
        logger.debug('raw_counts:\n%s', raw_counts)
        counts = pd.read_table(StringIO.StringIO(raw_counts), '\\s', 
                                         names=['count'] + fields, parse_dates=[1])
        logger.debug('counts:\n %s', counts)
        os.remove(fout_name)
        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)
        cache_fname = os.path.join(cache_dir, os.path.split(fname)[1] + '.counts')
        cache_file = open(cache_fname, 'w')
        counts.to_csv(cache_file, index=False)
        cache_file.close()
        logger.debug('wrote file to: %s', cache_fname)
        return counts
    except:
        logger.exception('caught exception in subprocess:')
        raise
        

def count_views(fnames, cache, cache_dir, filter_by_mobile):
    pool = multiprocessing.Pool(min(len(fnames),20))
    counts = pool.map(count_file, zip(fnames, itertools.repeat(cache_dir), itertools.repeat(filter_by_mobile)))
    logger.debug('len(counts): %d', len(counts))
    for file_counts in counts:
        logger.debug('file_counts:\n%s', file_counts)
        cache = cache.append(file_counts, ignore_index=True)
        
    # aggregate by date, lang, project, site, country
    total = cache.groupby(fields).sum()
    logger.debug('total counts:\n%s', total)
    return total


def get_files(datadir):
    logger.info('walking datadir: %s', datadir)
    match_files = []
    for root, dirs, files in os.walk(datadir):
        for f in files:
            # logger.debug('matching pattern against file: %s', f)
            m = re.match('[a-z0-9\-]+\.log-\d{8}.gz', f)
            if m:
                match_files.append(os.path.join(root, f))
    # logging.debug('match_files: %s', match_files)
    return match_files


def get_counts(basedir, cache_dir, cache_only, filter_by_mobile):
    cache = pd.DataFrame(columns=['count'] + fields)
    if os.path.exists(cache_dir):
        for count_file in os.listdir(cache_dir):
            try:
                df = pd.read_table(os.path.join(cache_dir, count_file), parse_dates=['date'], sep=',')
                # logging.debug('cached file reloaded:\n%s', df[:10])
                cache = cache.append(df)
            except:
                logger.exception('exception caught while loading cache file: %s', count_file)
    logger.debug('cache:\n%s', cache)

    files = get_files(basedir)
    # -7 because '.counts' is 7 chars long
    cached_files = map(lambda f : f[:-7], os.listdir(cache_dir)) if os.path.exists(cache_dir) else []
    filtered = filter(lambda f : f not in cached_files, files)
    logger.debug('len(filtered): %d', len(filtered))

    if not cache_only:
        counts = count_views(filtered, cache, cache_dir, filter_by_mobile)
    counts = cache
    # logger.debug('counts:\n%s', counts[:10])
    return counts


def make_dashboards(graphs, providers, basedir):
    for provider in providers:
        title_provider = provider.replace('-', ' ').title()
        underscore_provider = provider.replace('-', '_')
        name = '%s Wikipedia Zero Dashboard' % title_provider
        headline = title_provider
        subhead = 'Wikipedia Zero Dashboard'
        fname = provider + '.json'
        # db = copy.deepcopy(limnpy.Dashboard(name, headline, subhead, fname))
        db = limnpy.Dashboard(name, headline, subhead, fname)
        db.add_tab('Versions', ['%s_daily_version' % underscore_provider, '%s_monthly_version' % underscore_provider])
        db.add_tab('Country Fraction')#, ['%s_fraction_daily', '%s_fraction_monthly'])
        db.add_tab('Country Raw')#, ['%s_raw_daily', '%s_raw_monthly'])
        db.write(basedir)
    

def make_graphs(counts, providers, metadata, basedir):
    graphs = defaultdict(list)
    for provider in providers:
        prov_counts = counts[counts.provider == provider]
        if len(prov_counts) == 0:
            logger.warning('skipping provider: %s--graphs will not be available on dashbaord', provider)
            continue
        logger.debug('prov_counts:\n%s', prov_counts)

        daily_version = prov_counts.groupby(['date', 'site'], as_index=False).sum()
        daily_version_limn = daily_version.pivot('date', 'site', 'count')
        # logger.debug('daily_version_limn (metadata):\n%s', daily_version_limn)
        # logger.debug('daily_Version_limn (raw):\n%s', daily_version_limn[:10])
        daily_version_source = limnpy.DataSource('%s_daily_version' % provider.replace('-','_'), 
                                                 '%s Daily Version Counts' % provider.replace('-',' ').title(),
                                                 daily_version_limn)
        # logger.debug('daily_version_source: %s', daily_version_source)
        daily_version_source.write(basedir)
        daily_version_graph = daily_version_source.get_graph(['Z','M'])
        daily_version_graph.__graph__['options']['stackedGraph'] = True
        daily_version_graph.write(basedir)
        graphs[provider].append(daily_version_graph)

        monthly_version = daily_version_limn.resample(rule='M', how='sum', label='right')
        # logger.debug('monthly_version:\n%s', monthly_version)
        monthly_version_source = limnpy.DataSource('%s_monthly_version' % provider.replace('-','_'), 
                                                   '%s Monthly Version Counts' % provider.replace('-',' ').title(),
                                                   monthly_version.reset_index())
        monthly_version_source.write(basedir)
        monthly_version_graph = monthly_version_source.get_graph(['Z','M'])
        monthly_version_graph.__graph__['options']['stackedGraph'] = True
        monthly_version_graph.write(basedir)
        graphs[provider].append(monthly_version_graph)

    return graphs


def parse_args():
    parser = argparse.ArgumentParser(description='Process a collection of squid logs and write certain extracted metrics to file')
    parser.add_argument('-zd', '--zero_datadir',    
                        dest='zero_datadir',
                        default='/a/squid/archive/zero/',
                        help='the top-level directory from which to recursively descend '
                        'in search of squid logs from the zero filters')
    parser.add_argument('-md', '--mobile_datadir',    
                        dest='mobile_datadir',
                        default='/a/squid/archive/sampled/',
                        help='the top-level directory from which to recursively descend '
                        'in search of the general squid logs')                        
    parser.add_argument('-l',
                        '--log',
                        dest='log_level',
                        choices=LEVELS.keys(),
                        default='DEBUG',
                        help='log level')
    parser.add_argument('--zero_cache',
                        default='zero_counts',
                        help='file in which to save historical counts of zero filtered request.  each run just appends to this file.')
    parser.add_argument('--mobile_cache',
                        default='mobile_counts.csv',
                        help='file in which to save historical counts for entire mobile site.  each run just appends to this file.')
    parser.add_argument('--metadata',
                        default='WP Zero Partner - Versions')
    parser.add_argument('--providers',
                        nargs='+',
                        default=DEFAULT_PROVIDERS,
                        help='list of providers for which to create dashboards')
    parser.add_argument('--cache_only',
                        default=False,
                        action='store_true',
                        help='specifies wether to compute new counts or just use the available cache')
    parser.add_argument('--limn_basedir',
                        default='data',
                        help='basedir in which to place limn datasource/datafile/graphs/dashboards directories')

    args = parser.parse_args()
    opts = vars(args)
    logger.setLevel(LEVELS.get(opts['log_level']))
    logger.info('\n' + pprint.pformat(opts))
    return opts


def main():
    opts = parse_args()
    zero_counts = get_counts(opts['zero_datadir'], opts['zero_cache'], cache_only=opts['cache_only'], filter_by_mobile=False)
    graphs = make_graphs(zero_counts, opts['providers'], opts['metadata'], opts['limn_basedir'])
    make_dashboards(graphs, opts['providers'], opts['limn_basedir'])
    
    

if __name__ == '__main__':
    main()
