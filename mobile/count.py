#!/usr/bin/python 
"""
logging set up
"""
import logging
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)-5.5s][%(name)-10.10s][%(processName)-14.14s][%(funcName)15.15s:%(lineno)4.4d]\t%(message)s')
#formatter = logging.Formatter('[%(levelname)s]\t[%(name)s]\t[%(processName)s]\t[%(funcName)s:%(lineno)s]\t%(message)s')

ch.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
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
from collections import defaultdict, Counter
import copy

import sqproc
import gcat
import limnpy

DEFAULT_PROVIDERS = ['digi-malaysia',
                     'grameenphone-bangladesh',
                     'orange-kenya',
                     'orange-niger',
                     'orange-tunisia',
                     'orange-uganda',
                     'orange-cameroon',
                     'orange-ivory-coast',
                     'telenor-montenegro',
                     'tata-india',
                     'saudi-telecom',
                     'dtac-thailand']

PROVIDER_COUNTRY_CODES = {'digi-malaysia' : 'MY',
             'grameenphone-bangladesh' : 'BD',
             'orange-kenya' : 'KE',
             'orange-niger' : 'NE',
             'orange-tunisia' : 'TN',
             'orange-uganda' : 'UG',
             'orange-cameroon' : 'CM',
             'orange-ivory-coast' : 'CI',
             'telenor-montenegro' : 'ME',
             'tata-india' : 'IN',
             'saudi-telecom' : 'SA',
             'dtac-thailand' : 'TH'}

COUNTRY_NAMES = {'MY' : 'Malaysia',
                 'BD' : 'Bangladesh',
                 'KE' : 'Kenya',
                 'NE' : 'Niger',
                 'TN' : 'Tunisia',
                 'UG' : 'Uganda',
                 'CM' : 'Cameroon',
                 'CI' : 'Ivory Coast',
                 'ME' : 'Montenegro',
                 'IN' : 'India',
                 'SA' : 'Saudi Arabia',
                 'TH' : 'Thailand'}


LEVELS = {'DEBUG': logging.DEBUG,
          'INFO': logging.INFO,
          'WARNING': logging.WARNING,
          'ERROR': logging.ERROR,
          'CRITICAL': logging.CRITICAL}


fields = ['date', 'lang', 'project', 'site', 'country', 'provider']


def count_file((fname, cache_dir, filter_by_mobile)):
    logger.debug('processing file: %s', fname)
    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)
    cache_fname = os.path.join(cache_dir, os.path.split(fname)[1] + '.counts')
    try:
        with open(cache_fname, 'w') as cache_file: # create the output file at the beginning as a simple lock

            fin = gzip.GzipFile(fname)
            ctr = Counter()
            for i, line in enumerate(fin):
                if i % 100000 == 0:
                    logger.debug('processed %d lines', i)
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

                ctr[tuple(map(str, map(row.__getitem__, fields)))] += 1

            raw_counts = map(lambda (key, count) : [count] + list(key), ctr.items())
            counts = pd.DataFrame(raw_counts, parse_dates=[1])
            logger.debug('counts:\n %s', counts)
            counts.to_csv(cache_file, index=False)
            cache_file.close()
            logger.debug('wrote file to: %s', cache_fname)
            return counts
    except:
        logger.exception('caught exception in subprocess:')
        os.remove(cache_fname)
        raise

        
        

def count_views(fnames, cache, cache_dir, filter_by_mobile):
    pool = multiprocessing.Pool(min(len(fnames),2))
    counts = pool.map_async(count_file, zip(fnames, itertools.repeat(cache_dir), itertools.repeat(filter_by_mobile))).get(float('inf'))
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

    if not cache_only:
        files = get_files(basedir)
        # -7 because '.counts' is 7 chars long
        cached_files = map(lambda f : f[:-7], os.listdir(cache_dir)) if os.path.exists(cache_dir) else []
        filtered = filter(lambda f : f not in cached_files, files)
        logger.debug('len(filtered): %d', len(filtered))
        counts = count_views(filtered, cache, cache_dir, filter_by_mobile)
    else:
        counts = cache
    # logger.debug('counts:\n%s', counts[:10])
    return counts


def make_sampled_source(counts, basedir):
    daily_country_counts = counts.groupby(['country', 'date'], as_index=False).sum()
    logger.debug('daily_country_counts (meta): %s', daily_country_counts)
    logger.debug('daily_country_counts (data): %s', daily_country_counts[:10])
    daily_country_counts_limn = daily_country_counts.pivot('date', 'country', 'count')
    daily_country_source = limnpy.DataSource('daily_mobile_views_by_country',
                                             'Daily Mobile Views By Country',
                                             daily_country_counts_limn)
    daily_country_source.write(basedir)

    monthly_country_counts = daily_country_counts_limn.resample(rule='M', how='sum', label='right')
    monthly_country_source = limnpy.DataSource('monthly_mobile_views_by_country',
                                               'Monthly Mobile Views By Country',
                                               monthly_country_counts)
    monthly_country_source.write(basedir)
    return daily_country_source, monthly_country_source
    

def make_zero_sources(counts, provider, basedir):
    provider_underscore = provider.replace('-','_')
    provider_title = provider.replace('-',' ').title()

    prov_counts = counts[counts.provider == provider]
    if len(prov_counts) == 0:
        logger.warning('skipping provider: %s--graphs will not be available on dashbaord', provider)
        return None, None
    logger.debug('prov_counts:\n%s', prov_counts)

    daily_version = prov_counts.groupby(['date', 'site'], as_index=False).sum()
    daily_version_limn = daily_version.pivot('date', 'site', 'count')
    # logger.debug('daily_version_limn (metadata):\n%s', daily_version_limn)
    # logger.debug('daily_Version_limn (raw):\n%s', daily_version_limn[:10])
    daily_version_source = limnpy.DataSource('%s_daily_version' % provider_underscore,
                                             '%s Daily Version Counts' % provider_title,
                                             daily_version_limn)
    daily_version_source.write(basedir)
    logger.debug('daily_version_source: %s', daily_version_source)

    monthly_version = daily_version_limn.resample(rule='M', how='sum', label='right')
    # logger.debug('monthly_version:\n%s', monthly_version)
    monthly_version_source = limnpy.DataSource('%s_monthly_version' % provider_underscore,
                                               '%s Monthly Version Counts' % provider_title,
                                               monthly_version.reset_index())
    monthly_version_source.write(basedir)
    return daily_version_source, monthly_version_source


def make_percent_sources(provider, 
                          daily_country_source, 
                          monthly_country_source, 
                          daily_version_source, 
                          monthly_version_source,
                          basedir):
    provider_underscore = provider.replace('-','_')
    provider_title = provider.replace('-',' ').title()

    cc = PROVIDER_COUNTRY_CODES[provider]
    country = COUNTRY_NAMES[cc]

    logger.debug('type(daily_version_source.__data__: %s)', type(daily_version_source.__data__))
    # logger.debug('daily_version_source.__data__: %s', daily_version_source.__data__)
    if 'M' not in daily_version_source.__data__.columns or 'Z' not in daily_version_source.__data__.columns:
        logger.warning('skipping count graphs for provider %s because M or Z column is missing from daily_version_source', provider)
        return None, None
    percent_df = daily_version_source.__data__[['M', 'Z']].sum(axis=1)
    logger.debug('type(percent_df): %s', type(percent_df))
    percent_df = pd.DataFrame(percent_df / daily_country_source.__data__[cc])
    percent_df = percent_df * 100
    percent_df = percent_df.reset_index()
    percent_df.columns = ['date', 'country_percent']
    logger.debug('type(percent_df): %s', type(percent_df))
    logger.debug('percent_df (meta):\n%s', percent_df)
    logger.debug('percent_df (data):\n%s', percent_df[:10])
    daily_percent_source = limnpy.DataSource('%s_daily_country_percent' % provider_underscore,
                                              '%s Daily Country Percent' % provider_title,
                                              percent_df)
    daily_percent_source.write(basedir)

    monthly_percent_source = limnpy.DataSource('%s_monthly_country_percent' % provider_underscore,
                                                '%s Monthly Country Percent' % provider_title,
                                                percent_df.resample(rule='M', how='sum', label='right'))
    monthly_percent_source.write(basedir)
    
    return daily_percent_source, monthly_percent_source


def make_graphs(counts, sampled_counts, providers, basedir):
    daily_country_source, monthly_country_source = make_sampled_source(counts, basedir)
    for provider in providers:

        provider_title = provider.replace('-', ' ').title()
        provider_underscore = provider.replace('-', '_')


        # construct dashboard object #################################
        name = '%s Wikipedia Zero Dashboard' % provider_title
        db = limnpy.Dashboard(provider, name, headline=provider_title, subhead='Wikipedia Zero Dashboard')

        daily_version_source, monthly_version_source = make_zero_sources(counts, provider, basedir)
        if daily_version_source is None:
            logger.warning('daily_version_source is None; skipping %s', provider)
            db.write(basedir)
            continue

        # main tab ###################################################
        daily_version_graph = daily_version_source.get_graph(['Z','M'])
        daily_version_graph.__graph__['options']['stackedGraph'] = True
        daily_version_graph.write(basedir)

        monthly_version_graph = monthly_version_source.get_graph(['Z','M'])
        monthly_version_graph.__graph__['options']['stackedGraph'] = True
        monthly_version_graph.write(basedir)

        db.add_tab('Versions', [daily_version_graph.__graph__['slug'], monthly_version_graph.__graph__['slug']])


        logger.debug('starting to create country count tabs')
        cc = PROVIDER_COUNTRY_CODES[provider]
        country = COUNTRY_NAMES[cc]


        # country raw tab ############################################
        # def __init__(self, id, title, sources, metric_ids=None, slug=None):
        daily_country_graph = limnpy.Graph('%s_daily_country_total' % provider_underscore,
                                           '%s Daily Views Raw' % country,
                                           sources=[daily_version_source, daily_country_source],
                                           metric_ids=[(daily_version_source.__source__['id'], 'M'),
                                                       (daily_country_source.__source__['id'], cc)])
        logger.debug('constructed daily_country_graph')
        monthly_country_graph = limnpy.Graph('%s_monthly_country_total' % provider_underscore,
                                           '%s Monthly Views Raw' % country,
                                           sources=[monthly_version_source, monthly_country_source],
                                           metric_ids=[(monthly_version_source.__source__['id'], 'M'),
                                                       (monthly_country_source.__source__['id'], cc)])

        logger.debug('constructed monthly_country_graph')
        db.add_tab('Country Raw', [daily_country_graph.__graph__['slug'], monthly_country_graph.__graph__['slug']])


        # country percent tab ########################################
        daily_percent_source, monthly_percent_source = make_percent_sources(provider, 
                                                                               daily_country_source,
                                                                               monthly_country_source,
                                                                               daily_version_source,
                                                                               monthly_version_source,
                                                                               basedir)
        if daily_percent_source is None:
            logger.warning('daily_percent_source is None; skipping %s', provider)
            continue

        daily_percent_graph = daily_percent_source.get_graph()
        daily_percent_graph.write(basedir)

        monthly_percent_graph = monthly_percent_source.get_graph()
        monthly_percent_graph.write(basedir)

        db.add_tab('Country Percent', [daily_percent_graph.__graph__['slug'], monthly_percent_graph.__graph__['slug']])
        db.write(basedir)
        



def parse_args():
    parser = argparse.ArgumentParser(description='Process a collection of squid logs and write certain extracted metrics to file')
    parser.add_argument('--zero_datadir',    
                        default='/a/squid/archive/zero/',
                        help='the top-level directory from which to recursively descend '
                        'in search of squid logs from the zero filters')
    parser.add_argument('--sampled_datadir',    
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
    parser.add_argument('--sampled_cache',
                        default='sampled_counts',
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
    sampled_counts = get_counts(opts['sampled_datadir'], opts['sampled_cache'], cache_only=opts['cache_only'], filter_by_mobile=True)
    zero_counts = get_counts(opts['zero_datadir'], opts['zero_cache'], cache_only=opts['cache_only'], filter_by_mobile=False)
    make_graphs(zero_counts, sampled_counts, opts['providers'], opts['limn_basedir'])
    

if __name__ == '__main__':
    main()
