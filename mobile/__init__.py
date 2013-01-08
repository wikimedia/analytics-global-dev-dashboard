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
import math
import gzip
import subprocess, multiprocessing, signal
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

SQUID_DATE_FMT = '%Y%m%d'

VERSIONS = {'X' : 'X', 'M' : 'M', 'Z' : 'Z', 'Country' : 'Country'}
VERSIONS_LONG = {'X' : 'Page views from partner to desktop (non-mobile) Wikipedia urls',
                'M' : 'Page views from partner to m.wikipedia.org urls',
                'Z' : 'Page views from partner to zero.wikipedia.org',
                'M+Z' : 'Combined page views from partner to m.wikipedia.org and zero.wikipedia.org urls',
                'Country' : 'Total page views within country (all networks, not just partner) to m.wikipedia.org and zero.wikipedia.org urls'}

FIELDS = ['date', 'lang', 'project', 'site', 'country', 'provider']
COUNT_FIELDS = ['count'] + FIELDS


def make_extended_legend(versions, country='country', partner='partner'):
    table = lambda s : '<table vspace=\"20\">%s</table>' % s
    row = lambda s : '<tr>%s</tr>' % s
    cell = lambda s : '<td>%s</td>' % s
    top_right_cell = lambda s : '<td valign=\"top\" align=\"right\">%s</td>' % s
    country_replace = lambda s : s.replace('country', country)
    partner_replace = lambda s : s.replace('partner', partner)
    bold = lambda s : '<strong>%s</strong>' % s
    title = lambda s : ' \n <h3>Extended Legend<h3> \n %s' % s

    rows = ''
    for v in versions:
        cells = top_right_cell(bold(VERSIONS[v])) + cell(VERSIONS_LONG[v])
        rows += row(cells)
    t = table(rows)
    final = title(t)
    final = country_replace(final)
    final = partner_replace(final)
    return final


def count_file((fname, cache_dir, filter_by_mobile)):
    logger.debug('processing file: %s', fname)
    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)
    cache_fname = os.path.join(cache_dir, os.path.split(fname)[1] + '.counts')
    try:
        cache_file = open(cache_fname, 'w')

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
        date_col_ind = COUNT_FIELDS.index('date')
        counts[date_col_ind] = pd.to_datetime(counts[date_col_ind])
        logger.debug('counts:\n %s', counts)
        counts.to_csv(cache_file, index=False, header=False)
        cache_file.close()
        logger.debug('wrote file to: %s', cache_fname)
        return (fname, counts)
    except:
        logger.exception('caught exception in subprocess:')
        os.remove(cache_fname)
        raise

        
def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)    

def clean_up(finished_fnames, all_fnames, cache_dir):
    pass

def count_views(fnames, cache_dir, filter_by_mobile):
    cache = pd.DataFrame(columns=COUNT_FIELDS)
    pool = multiprocessing.Pool(min(len(fnames),1), init_worker)
    # counts = pool.imap(count_file, zip(fnames, itertools.repeat(cache_dir), itertools.repeat(filter_by_mobile))).get(float('inf'))
    # finished, counts = zip(*counts)
    counts = []
    finished_fnames = []
    try:
        for fname, count in pool.imap(count_file, zip(fnames, itertools.repeat(cache_dir), itertools.repeat(filter_by_mobile))):
            counts.append(count)
            finished_fnames.append(fname)
    except KeyboardInterrupt:
        logger.exception('Caught KeyboardInterrupt, cleaning up unfinished files')
        clean_up(finished_fnames, fnames, cache_dir)

    logger.debug('len(counts): %d', len(counts))
    for file_counts in counts:
        logger.debug('file_counts:\n%s', file_counts)
        cache = cache.append(file_counts, ignore_index=True)
        
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


def get_missing_files(cache_dir):
    dates = []
    fnames = os.listdir(cache_dir)
    for f in fnames:
        m = re.match('.*\.log-(\d{8}).*', f)
        if m:
            dates.append(m.groups(1)[0])
        else:
            logger.warning('failed to parse date from filename: %s', f)
    start = min(dates)
    end = max(dates)
    drange = pd.date_range(start, end)
    drange_str = map(lambda d : d.strftime(SQUID_DATE_FMT), drange)
    missing = list(set(drange_str) - set(dates))
    if missing:
        logger.warning('found %d missing dates in cache_dir: %s, missing dates:\n%s', len(missing), cache_dir, missing)
    return missing
    

def clear_missing_days(cache, missing):
    orig = cache
    for dstr in missing:
        d = datetime.datetime.strptime(dstr, SQUID_DATE_FMT)
        logger.debug('cache[cache.date == d]: %s', cache[cache.date == d])
        cache = cache[cache.date != d]
    if len(cache) != len(orig):
        logger.debug('entered with cache: %s', orig)
        logger.debug('exiting with cache: %s', cache)
    return cache


def get_counts(basedir, cache_dir, sample_rate, filter_by_mobile, cache_only):
    counts = pd.DataFrame(columns=COUNT_FIELDS)
    date_col_ind = COUNT_FIELDS.index('date')
    num_cached = len(os.listdir(cache_dir))
    logger.info('loading %d cached files from %s', num_cached, cache_dir)

    # COUNT NEW FILES
    if not cache_only:
        files = get_files(basedir)
        # -7 because '.counts' is 7 chars long
        cached_files = map(lambda f : f[:-len('.counts')], os.listdir(cache_dir)) if os.path.exists(cache_dir) else []
        filtered = filter(lambda f : f not in cached_files, files)
        logger.info('parsing  %d files from %s', len(filtered), cache_dir)
        counts = count_views(filtered, cache_dir, filter_by_mobile)

    # LOAD CACHED COUNTS
    if os.path.exists(cache_dir):
        for i, count_file in enumerate(os.listdir(cache_dir)):
            # if i > 2:
            #     break
            try:
                df = pd.read_table(os.path.join(cache_dir, count_file), parse_dates=[date_col_ind], sep=',',  header=None)
                df.columns = COUNT_FIELDS
                logging.debug('loaded %d lines from %s (%d / %d)', len(df), count_file, i, num_cached)
                counts = counts.append(df)
            except:
                logger.exception('exception caught while loading cache file: %s', count_file)
    logger.debug('counts:\n%s', counts)

    missing = get_missing_files(cache_dir)
    counts = clear_missing_days(counts, missing)

    # deal with that fact that the same record will sometimes be split between two files
    # and thus wind up as two different rows in the dataframe
    # to solve this, just aggregate by all fields
    counts = counts.groupby(FIELDS)['count'].sum().reset_index()
    logger.debug('aggregated counts:\n%s', counts[:-1])

    # scale to compensate for filtering
    counts['count'] = counts['count'] * sample_rate
    counts = counts[counts['project'] == 'wikipedia']
    # logger.debug('counts:\n%s', counts[:10])
    return counts


def make_sampled_source(counts, basedir):
    daily_country_counts = counts.groupby(['country', 'date'], as_index=False).sum()
    logger.debug('daily_country_counts: %s', daily_country_counts)
    daily_country_counts_limn = daily_country_counts.pivot('date', 'country', 'count')
    daily_country_counts_limn = daily_country_counts_limn.rename(columns=COUNTRY_NAMES)
    daily_country_counts_limn = daily_country_counts_limn[:-1]
    daily_country_source = limnpy.DataSource('daily_mobile_wp_views_by_country',
                                             'Daily Mobile WP Views By Country',
                                             daily_country_counts_limn)
    daily_country_source.write(basedir)

    monthly_country_counts = daily_country_counts_limn.resample(rule='M', how='sum', label='right')
    monthly_country_counts = monthly_country_counts[:-1]
    monthly_country_source = limnpy.DataSource('monthly_mobile_wp_views_by_country',
                                               'Monthly Mobile WP Views By Country',
                                               monthly_country_counts)
    monthly_country_source.write(basedir)
    return daily_country_source, monthly_country_source
    

def make_zero_sources(counts, provider, basedir):
    provider_underscore = provider.replace('-','_')
    provider_title = provider.replace('-',' ').title()

    # logger.debug('counts:\n%s', counts)
    # logger.debug('counts.columns: %s', counts.columns)

    # HACK TO FIX SPELLING ERRER in orange-tunisia udp-filter
    provider_id = provider if provider != 'orange-tunisia' else 'orange-tunesia'
    prov_counts = counts[counts.provider == provider_id]
    if len(prov_counts) == 0:
        logger.warning('skipping provider: %s--graphs will not be available on dashbaord', provider)
        return None, None

    daily_version = prov_counts.groupby(['date', 'site'], as_index=False).sum()
    daily_version_limn = daily_version.pivot('date', 'site', 'count')
    daily_version_limn = daily_version_limn.rename(columns=VERSIONS)
    daily_version_limn = daily_version_limn[:-1]
    daily_version_source = limnpy.DataSource('%s_daily_wp_view_by_version' % provider_underscore,
                                             '%s Daily WP Views By Version' % provider_title,
                                             daily_version_limn)
    daily_version_source.write(basedir)

    monthly_version = daily_version_limn.resample(rule='M', how='sum', label='right')
    monthly_version = monthly_version[:-1]
    monthly_version_source = limnpy.DataSource('%s_monthly_wp_views_by_version' % provider_underscore,
                                               '%s Monthly WP View By Version' % provider_title,
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
    this function is necessary because putting two datasources together with different
    time ranges breaks the current (old) limn.  so this uses pandas to align them and create
    a special graph-specific datasource
    """
    provider_underscore = provider.replace('-','_')
    provider_title = provider.replace('-',' ').title()

    cc = PROVIDER_COUNTRY_CODES[provider]
    country = COUNTRY_NAMES[cc]

    # joins on date index because limnpy.DataSource DataFrames are always indexed on date
    daily_raw_df = pd.merge(daily_version_source.__data__,
                            pd.DataFrame(daily_country_source.__data__[country]),
                            left_index=True, right_index=True)
    # add a combined M + Z source
    if VERSIONS['M'] in daily_raw_df.columns and VERSIONS['Z'] in daily_raw_df.columns:
        daily_raw_df['M + Z'] = daily_raw_df[VERSIONS['M']] + daily_raw_df[VERSIONS['Z']]
    daily_raw_df = daily_raw_df[:-1]
    daily_raw_source = limnpy.DataSource('%s_daily_wp_views_with_country' % provider_underscore,
                                         '%s Daily WP Views With Total %s WP Views' % (provider_title, country),
                                         daily_raw_df)
    daily_raw_source.write(basedir)

    monthly_raw_df = pd.merge(monthly_version_source.__data__, 
                              pd.DataFrame(monthly_country_source.__data__[country]),
                              left_index=True, right_index=True)
    if VERSIONS['M'] in monthly_raw_df.columns and VERSIONS['Z'] in monthly_raw_df.columns:
        monthly_raw_df['M + Z : Mobile + Zero'] = monthly_raw_df[VERSIONS['M']] + monthly_raw_df[VERSIONS['Z']]
    monthly_raw_df = monthly_raw_df[:-1]
    monthly_raw_source = limnpy.DataSource('%s_monthly_wp_views_with_country' % provider_underscore,
                                           '%s Monthly WP Views With Total %s WP Views' % (provider_title, country),
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
    available_versions = list(set([VERSIONS['M'], VERSIONS['Z']]) & set(daily_version_source.__data__.columns))

    logger.debug('type(daily_version_source.__data__: %s)', type(daily_version_source.__data__))
    # logger.debug('daily_version_source.__data__: %s', daily_version_source.__data__)

    daily_percent_df = daily_version_source.__data__[available_versions].sum(axis=1)
    # doesn't work because it is usually log files which are missing which don't match to timestamp days
    # daily_percent_df = daily_percent_df.ix[daily_country_source.__data__[cc].index]
    daily_percent_df = pd.DataFrame(daily_percent_df / daily_country_source.__data__[country])
    # daily_percent_df = pd.DataFrame(daily_percent_df[daily_percent_df[0] < 1])
    daily_percent_df = daily_percent_df * 100
    daily_percent_df = daily_percent_df.reset_index()
    daily_percent_df.columns = ['date', 'Country Percentage Share']
    daily_percent_source = limnpy.DataSource('%s_daily_wp_views_as_percent_country_share' % provider_underscore,
                                              '%s Daily WP Views as Percentage Share of %s WP Views' % (provider_title,country),
                                              daily_percent_df)
    daily_percent_source.write(basedir)

    # can't just aggregate daily percents--math doesn't work like that
    monthly_percent_df = monthly_version_source.__data__[available_versions].sum(axis=1)
    monthly_percent_df = pd.DataFrame(monthly_percent_df / monthly_country_source.__data__[country])
    monthly_percent_df = monthly_percent_df * 100
    monthly_percent_df = monthly_percent_df.reset_index()
    monthly_percent_df.columns = ['date', 'country_percent']
    monthly_percent_source = limnpy.DataSource('%s_monthly_wp_views_as_percent_country_share' % provider_underscore,
                                              '%s Monthly WP Views as Percentage Share of %s WP Views' % (provider_title, country),
                                              monthly_percent_df)
    monthly_percent_source.write(basedir)


    return daily_percent_source, monthly_percent_source

def make_summary(datasources, basedir):
    vm = gcat.get_file('WP Zero Partner - Versions', fmt='pandas', usecache=True)
    vm = vm.set_index(vm['Partner Identifier'])
    logging.info('type(vm.ix[1,\'Start Date\']: %s', type(vm.ix[1,'Start Date']))
    dfs = []
    for provider, datasource in datasources.items():
        # logger.debug('vm: %s', vm)
        start_date = vm.ix[provider, 'Start Date']
        if not start_date or (isinstance(start_date, float) and math.isnan(start_date)): # this means that the provider isn't yet live
            continue
        valid_df = datasource.__data__[datasource.__data__.index > start_date]
        # logger.debug('valid_df: %s', valid_df)

        free_versions = set(map(VERSIONS.get, map(unicode.strip, vm.ix[provider, 'Version'].split(','))))
        valid_df = valid_df[list(free_versions & set(valid_df.columns))]
        # logger.debug('valid_df: %s', valid_df)
        dfs.append(valid_df)
        
    long_fmt = pd.concat(dfs)
    # logger.debug('long_fmt: %s', long_fmt)
    long_fmt = long_fmt.reset_index()
    long_fmt = long_fmt.rename(columns={'index' : 'date'})
    # logger.debug('long_fmt: %s', long_fmt)
    # logger.debug('long_fmt.columns: %s', long_fmt.columns)
    final = long_fmt.groupby('date').sum()
    final['All Versions'] = final.sum(axis=1)
    final = final[:-1]
    # logger.debug('final: %s', final)
    total_ds = limnpy.DataSource('free_mobile_traffic_by_version', 'Free Mobile Traffic by Version', final)
    total_ds.write(basedir)
    total_graph = total_ds.get_graph()
    total_graph.__graph__['desc'] = "The [Wikipedia Zero](http://www.mediawiki.org/wiki/Wikipedia_Zero) initiative works with mobile phone operators to enable mobile access to wikipedia free of data charges.  Operators provide free access to either the [full mobile site](http://en.m.wikipedia.org) or the [mobile site without images](http://en.zero.wikipedia.org).  This graph shows the total number of free page requests coming from all of our mobile partners for each of those versions.  We only consider the requests for the versions to which each operator provides free access, and we only begin counting requests after the public start date for each operator."
    total_graph.__graph__['desc'] += make_extended_legend(['M+Z', 'M', 'Z'])
    total_graph.write(basedir)

    final_monthly = final.resample(rule='M', how='sum', label='right')
    final_monthly = final_monthly[:-1]
    total_ds_monthly = limnpy.DataSource('free_mobile_traffic_by_version_monthly', 'Monthly Free Mobile Traffic by Version', final_monthly)
    total_ds_monthly.write(basedir)
    total_graph_monthly = total_ds_monthly.get_graph()
    total_graph_monthly.__graph__['desc'] = total_graph.__graph__['desc']
    total_graph_monthly.write(basedir)
    


def make_graphs(counts, sampled_counts, providers, basedir):
    daily_country_source, monthly_country_source = make_sampled_source(sampled_counts, basedir)
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
        daily_version_graph = daily_version_source.get_graph([VERSIONS['M'], VERSIONS['Z']])
        #daily_version_graph.__graph__['options']['stackedGraph'] = True
        daily_version_graph.__graph__['desc'] = make_extended_legend(['M', 'Z'])
        daily_version_graph.write(basedir)


        monthly_version_graph = monthly_version_source.get_graph([VERSIONS['M'], VERSIONS['Z']])
        #monthly_version_graph.__graph__['options']['stackedGraph'] = True
        monthly_version_graph.__graph__['desc'] = "This graph is identical to the daily version except that it has been "\
        "aggregated by month.  Each monthly bin is backwards looking, meaning that the aggregate counts are "\
        "plotted as the last day of that month.  So the aggregate count for October would be plotted on October 31. "
        monthly_version_graph.__graph__['desc'] += make_extended_legend(['M','Z'])
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
        daily_raw_graph.__graph__['desc'] = "This graph compares the number of page requests from the provider IP ranges "\
        "for each wikipedia version with the total number of page views to the mobile site from the entire country of %s." % country
        daily_raw_graph.__graph__['desc'] +=  make_extended_legend(['X', 'M', 'Z', 'Country'], country=country, partner=provider)

        daily_raw_graph.write(basedir)

        monthly_raw_graph = monthly_raw_source.get_graph()
        monthly_raw_graph.__graph__['desc'] = "This graph is identical to the daily version except that it has been "\
        "aggregated by month.  Each monthly bin is backwards looking, meaning that the aggregate counts are "\
        "plotted as the last day of that month.  So the aggregate count for October would be plotted on October 31."
        monthly_raw_graph.__graph__['desc'] += make_extended_legend(['X', 'M', 'Z', 'Country'], country=country, partner=provider)
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
        daily_percent_graph.__graph__['desc'] = "This graph shows the number of page requests from the provider IP ranges "\
        "to the mobile and zero versions of wikipedia divided by the total number page views to those sites from the entire "\
        "country of %s." % country
        daily_percent_graph.write(basedir)

        monthly_percent_graph = monthly_percent_source.get_graph()
        monthly_percent_graph.__graph__['desc'] = "This graph is identical to the daily version except that it has been "\
        "aggregated by month.  Each monthly bin is backwards looking, meaning that the aggregate counts are "\
        "plotted as the last day of that month.  So the aggregate count for October would be plotted on October 31."
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
                             cache_only=opts['cache_only'])
                             # cache_only=True)

    sampled_counts = get_counts(opts['sampled_datadir'], 
                                opts['sampled_cache'], 
                                sample_rate=1000, 
                                filter_by_mobile=True,
                                cache_only=opts['cache_only'])
                                # cache_only=False)
    make_graphs(zero_counts, sampled_counts, opts['providers'], opts['limn_basedir'])
    

if __name__ == '__main__':
    main()
