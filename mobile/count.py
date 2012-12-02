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

VERSIONS = ['X', 'M', 'Z']

FIELDS = ['date', 'lang', 'project', 'site', 'country', 'provider']


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
                
                try:
                    ctr[tuple(map(str, map(row.__getitem__, FIELDS)))] += 1
                except:
                    logger.exception('error retrieving values from row: %s', row)

            raw_counts = map(lambda (key, count) : [count] + list(key), ctr.items())
            counts = pd.DataFrame(raw_counts)
            date_col_ind = FIELDS.index('date') + 1
            counts[date_col_ind] = pd.to_datetime(counts[date_col_ind])
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
    pool = multiprocessing.Pool(min(len(fnames),14))
    counts = pool.map_async(count_file, zip(fnames, itertools.repeat(cache_dir), itertools.repeat(filter_by_mobile))).get(float('inf'))
    logger.debug('len(counts): %d', len(counts))
    for file_counts in counts:
        logger.debug('file_counts:\n%s', file_counts)
        cache = cache.append(file_counts, ignore_index=True)
        
    # aggregate by date, lang, project, site, country
    total = cache.groupby(FIELDS).sum()
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


def get_counts(basedir, cache_dir, sample_rate, filter_by_mobile, cache_only):
    cache = pd.DataFrame(columns=['count'] + FIELDS)
    date_col_ind = FIELDS.index('date') + 1
    if os.path.exists(cache_dir):
        for count_file in os.listdir(cache_dir):
            try:
                df = pd.read_table(os.path.join(cache_dir, count_file), parse_dates=[date_col_ind], sep=',')
                df.columns = ['count'] + FIELDS
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

    # scale to compensate for filtering
    counts['count'] = counts['count'] * sample_rate
    # logger.debug('counts:\n%s', counts[:10])
    return counts


def make_sampled_source(counts, basedir):
    daily_country_counts = counts.groupby(['country', 'date'], as_index=False).sum()
    logger.debug('daily_country_counts: %s', daily_country_counts)
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

    daily_version = prov_counts.groupby(['date', 'site'], as_index=False).sum()
    daily_version_limn = daily_version.pivot('date', 'site', 'count')
    daily_version_source = limnpy.DataSource('%s_daily_version' % provider_underscore,
                                             '%s Daily Version Counts' % provider_title,
                                             daily_version_limn)
    daily_version_source.write(basedir)

    monthly_version = daily_version_limn.resample(rule='M', how='sum', label='right')
    monthly_version_source = limnpy.DataSource('%s_monthly_version' % provider_underscore,
                                               '%s Monthly Version Counts' % provider_title,
                                               monthly_version.reset_index())
    monthly_version_source.write(basedir)
    return daily_version_source, monthly_version_source


def make_raw_sources(provider, 
                          daily_country_source, 
                          monthly_country_source, 
                          daily_version_source, 
                          monthly_version_source,
                          basedir):
    """
    this function is necessary because putting two datasources together with differen
    time ranges break the current (old) limn.  so this uses pandas to align them and create
    a special datasource
    """
    provider_underscore = provider.replace('-','_')
    provider_title = provider.replace('-',' ').title()

    cc = PROVIDER_COUNTRY_CODES[provider]
    country = COUNTRY_NAMES[cc]

    # joins on date index because limnpy.DataSource DataFrames are always indexed on date
    daily_raw_df = pd.merge(daily_version_source.__data__,
                            pd.DataFrame(daily_country_source.__data__[cc]),
                            left_index=True, right_index=True)
    # add a combined M + Z source
    if 'M' in daily_raw_df.columns and 'Z' in daily_raw_df.columns:
        daily_raw_df['M + Z'] = daily_raw_df['M'] + daily_raw_df['Z']
    daily_raw_source = limnpy.DataSource('%s_daily_country_views' % provider_underscore,
                                         '%s Daily Country Views' % provider_title,
                                         daily_raw_df)
    daily_raw_source.write(basedir)

    monthly_raw_df = pd.merge(monthly_version_source.__data__, 
                              pd.DataFrame(monthly_country_source.__data__[cc]),
                              left_index=True, right_index=True)
    if 'M' in monthly_raw_df.columns and 'Z' in monthly_raw_df.columns:
        monthly_raw_df['M + Z'] = monthly_raw_df['M'] + monthly_raw_df['Z']
    monthly_raw_source = limnpy.DataSource('%s_monthly_country_views' % provider_underscore,
                                           '%s Monthly Country Views' % provider_title,
                                           monthly_raw_df)
    monthly_raw_source.write(basedir)

    return daily_raw_source, monthly_raw_source



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
    daily_percent_df = daily_version_source.__data__[['M', 'Z']].sum(axis=1)
    daily_percent_df = pd.DataFrame(daily_percent_df / daily_country_source.__data__[cc])
    daily_percent_df = daily_percent_df * 100
    daily_percent_df = daily_percent_df.reset_index()
    daily_percent_df.columns = ['date', 'country_percent']
    daily_percent_source = limnpy.DataSource('%s_daily_country_percent' % provider_underscore,
                                              '%s Daily Country Percent' % provider_title,
                                              daily_percent_df)
    daily_percent_source.write(basedir)

    # can't just aggregate daily percents--math doesn't work like that
    monthly_percent_df = monthly_version_source.__data__[['M', 'Z']].sum(axis=1)
    monthly_percent_df = pd.DataFrame(monthly_percent_df / monthly_country_source.__data__[cc])
    monthly_percent_df = monthly_percent_df * 100
    monthly_percent_df = monthly_percent_df.reset_index()
    monthly_percent_df.columns = ['date', 'country_percent']
    monthly_percent_source = limnpy.DataSource('%s_monthly_country_percent' % provider_underscore,
                                              '%s Monthly Country Percent' % provider_title,
                                              monthly_percent_df)
    monthly_percent_source.write(basedir)


    return daily_percent_source, monthly_percent_source

def make_summary(datasources, basedir):
    vm = gcat.get_file('WP Zero Partner - Versions', fmt='pandas', usecache=True)
    vm = vm.set_index(vm['Partner Identifier'])
    logging.info('type(vm.ix[1,\'Start Date\']: %s', type(vm.ix[1,'Start Date']))
    dfs = []
    for pid, datasource in datasources.items():
        start_date = vm.ix[pid, 'Start Date']
        if not start_date: # this means that the provider isn't yet live
            continue
        valid_df = datasource.__data__[datasource.__data__.index > start_date]
        # logger.debug('valid_df: %s', valid_df)

        free_versions = set(map(unicode.strip, vm.ix[pid, 'Version'].split(',')))
        valid_df = valid_df[list(set(free_versions) & set(valid_df.columns))]
        # logger.debug('valid_df: %s', valid_df)
        dfs.append(valid_df)
        
    long_fmt = pd.concat(dfs)
    logger.debug('long_fmt: %s', long_fmt)
    long_fmt = long_fmt.reset_index()
    long_fmt = long_fmt.rename(columns={'index' : 'date'})
    logger.debug('long_fmt: %s', long_fmt)
    logger.debug('long_fmt.columns: %s', long_fmt.columns)
    final = long_fmt.groupby('date').sum()
    final['All Versions'] = final.sum(axis=1)
    logger.debug('final: %s', final)
    total_ds = limnpy.DataSource('free_mobile_traffic_by_version', 'Free Mobile Traffic by Version', final)
    total_ds.write(basedir)
    total_ds.write_graph(basedir=basedir)


def make_graphs(counts, sampled_counts, providers, basedir):
    daily_country_source, monthly_country_source = make_sampled_source(sampled_counts, basedir)
    # daily_country_source, monthly_country_source = make_sampled_source(counts, basedir)
    provider_version_sources = {}
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
        provider_version_sources[provider] = daily_version_source

        # main tab ###################################################
        daily_version_graph = daily_version_source.get_graph(['Z','M'])
        #daily_version_graph.__graph__['options']['stackedGraph'] = True
        daily_version_graph.write(basedir)

        monthly_version_graph = monthly_version_source.get_graph(['Z','M'])
        #monthly_version_graph.__graph__['options']['stackedGraph'] = True
        monthly_version_graph.write(basedir)

        db.add_tab('Versions', [daily_version_graph.__graph__['slug'], monthly_version_graph.__graph__['slug']])


        logger.debug('starting to create country count tabs')
        cc = PROVIDER_COUNTRY_CODES[provider]
        country = COUNTRY_NAMES[cc]


        # country raw tab ############################################
        daily_raw_source, monthly_raw_source = make_raw_sources(provider, 
                                                                daily_country_source,
                                                                monthly_country_source,
                                                                daily_version_source,
                                                                monthly_version_source,
                                                                basedir)
        daily_raw_graph = daily_raw_source.get_graph()
        daily_raw_graph.write(basedir)

        monthly_raw_graph = monthly_raw_source.get_graph()
        monthly_raw_graph.write(basedir)
        db.add_tab('Country Raw', [daily_raw_graph.__graph__['slug'], monthly_raw_graph.__graph__['slug']])


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
    make_summary(provider_version_sources, basedir)



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

    zero_counts = get_counts(opts['zero_datadir'],
                             opts['zero_cache'],
                             sample_rate=10,
                             filter_by_mobile=False,
                             # cache_only=opts['cache_only'])
                             cache_only=True)

    sampled_counts = get_counts(opts['sampled_datadir'], 
                                opts['sampled_cache'], 
                                sample_rate=1000, 
                                filter_by_mobile=True,
                                # cache_only=opts['cache_only'])
                                cache_only=False)
    make_graphs(zero_counts, sampled_counts, opts['providers'], opts['limn_basedir'])
    

if __name__ == '__main__':
    main()
