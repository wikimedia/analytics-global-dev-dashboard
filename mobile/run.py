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


import os, argparse
import datetime
import math
import pprint
import re
import pandas as pd
import time
import unidecode
import copy

import gcat
import limnpy

#DEFAULT_PROVIDERS = ['digi-telecommunications-malaysia',
                     #'grameenphone-bangladesh',
                     #'orange-kenya',
                     #'orange-sahelc-niger',
                     #'orange-tunisia',
                     #'orange-uganda',
                     #'orange-cameroon',
                     #'orange-ivory-coast',
                     #'promonte-gsm-montenegro',
                     #'tata-india',
                     #'stc/al-jawal-saudi-arabia',
                     #'total-access-(dtac)-thailand']
                     ##'tim-brasil']

DEFAULT_PROVIDERS = ['digi-telecommunications-malaysia',
                     'grameenphone-bangladesh',
                     'orange-kenya',
                     'orange-sahelc-niger',
                     'orange-tunisia',
                     'orange-uganda',
                     'orange-cameroon',
                     'orange-ivory-coast',
                     'promonte-gsm-montenegro',
                     'stc/al-jawal-saudi-arabia',
                     'total-access-(dtac)-thailand']

PROVIDER_COUNTRY_CODES = {'digi-telecommunications-malaysia' : 'MY',
             'grameenphone-bangladesh' : 'BD',
             'orange-kenya' : 'KE',
             'orange-sahelc-niger' : 'NE',
             'orange-tunisia' : 'TN',
             'orange-uganda' : 'UG',
             'orange-cameroon' : 'CM',
             'orange-ivory-coast' : 'CI',
             'promonte-gsm-montenegro' : 'ME',
             'tata-india' : 'IN',
             'stc/al-jawal-saudi-arabia' : 'SA',
             'total-access-(dtac)-thailand' : 'TH',
             'tim-brasil' : 'BR'}

COUNTRY_NAMES = {'MY' : 'Malaysia',
                 'BD' : 'Bangladesh',
                 'BR' : 'Brazil',
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

LIMN_GROUP = 'gp'

LEVELS = {'DEBUG': logging.DEBUG,
          'INFO': logging.INFO,
          'WARNING': logging.WARNING,
          'ERROR': logging.ERROR,
          'CRITICAL': logging.CRITICAL}

SQUID_DATE_FMT = '%Y%m%d'

VERSIONS = {'X' : 'X', 'M' : 'M', 'M+Z' : 'M+Z', 'Z' : 'Z', 'Country' : 'Country'}
VERSIONS_LONG = {'X' : 'Free page views from partner to desktop (non-mobile) Wikipedia urls',
                'M' : 'Free page views from partner to m.wikipedia.org urls',
                'Z' : 'Free page views from partner to zero.wikipedia.org',
                'M+Z' : 'Combined free page views from partner to m.wikipedia.org and zero.wikipedia.org urls',
                'Country' : 'Total page views within country (all networks, not just partner) to m.wikipedia.org and zero.wikipedia.org urls'}

FIELDS = ['date', 'lang', 'project', 'site', 'country', 'provider']
#COUNT_FIELDS = ['count'] + FIELDS
COUNT_FIELDS = FIELDS + ['count']


def title(s):
    return s.replace('-', ' ').title() 

_punct_re = re.compile(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.]+')
def slugify(text, delim=u'_'):
    """Generates an ASCII-only slug. Credit to Armin Ronacher from
    http://flask.pocoo.org/snippets/5/"""
    result = []
    for word in _punct_re.split(text.lower()):
        result.extend(unidecode.unidecode(word).split())
    return unicode(delim.join(result))

def make_extended_legend(versions, country='country', partner='partner'):
    table = lambda s : '<table vspace=\"20\">%s</table>' % s
    row = lambda s : '<tr>%s</tr>' % s
    cell = lambda s : '<td>%s</td>' % s
    top_right_cell = lambda s : '<td valign=\"top\" align=\"right\">%s &nbsp;&nbsp; : &nbsp;&nbsp; </td>' % s
    country_replace = lambda s : s.replace('country', country)
    partner_replace = lambda s : s.replace('partner', partner)
    bold = lambda s : '<strong>%s  </strong>' % s
    title = lambda s : ' \n <h3>Extended Legend</h3> \n %s' % s

    rows = ''
    for v in versions:
        cells = top_right_cell(bold(VERSIONS[v])) + cell(VERSIONS_LONG[v])
        rows += row(cells)
    t = table(rows)
    final = title(t)
    final = country_replace(final)
    final = partner_replace(final)
    return final

def load_counts(cache_dir):
    counts = pd.DataFrame(columns=COUNT_FIELDS)
    date_col_ind = COUNT_FIELDS.index('date')

    num_cached = len(os.listdir(cache_dir))
    logger.info('loading %d cached files from %s', num_cached, cache_dir)

    i = 0
    for root, dirs, files in os.walk(cache_dir):
        for count_file in files:
            full_path = os.path.join(cache_dir, root, count_file)
            logger.debug('proecessing: %s', full_path)
            try:
                #df = pd.read_table(
                df = pd.read_csv(
                        os.path.join(root, full_path),
                        parse_dates=[date_col_ind],
                        date_parser=lambda s : datetime.datetime.strptime(s,
                            '%Y-%m-%d_%H'),
                            #'%Y-%m-%d'),
                        skiprows=1,
                        sep='\t',
                        names=COUNT_FIELDS)
                logging.debug('loaded %d lines from %s (%d / %d)', len(df), full_path, i, num_cached)
                counts = counts.append(df)
                i += 1
            except StopIteration: # this is what happens when Pandas tries to load an empty file
                pass
            except:
                logger.exception('exception caught while loading cache file: %s', count_file)

    # scale to compensate for filtering
    counts = counts[counts['project'] == 'wikipedia.org']
    #counts.date = counts.date.apply(lambda d : d.date())
    #counts = counts.pivot('date', [pivot_col, 'site'], 'count')
    #counts = counts.set_index('date').resample(rule='D', how='sum', label='right').reset_index()
    #counts = counts.groupby(FIELDS).count.sum().reset_index()
    logger.debug('loaded_counts:%s\n%s', counts, counts[:10])
    logger.debug('dates: %s', counts.date.unique())
    return counts


def get_provider_metadata(usecache=False):
    meta = None
    from apiclient.discovery import HttpError
    while meta is None:
        try:
            meta = gcat.get_file('WP Zero Partner - Versions', fmt='pandas', usecache=usecache)
        except HttpError:
            logger.warning('failed to connect to google drive server, trying again ...')
            time.sleep(1)
            meta = None
    meta = meta.set_index(meta['Partner Identifier'])
    return meta


def make_country_sources(counts, basedir):
    country_counts = counts.groupby(['country', 'date'], as_index=False).sum()
    country_counts_limn = country_counts.pivot('date', 'country', 'count')
    daily_country_counts_limn = country_counts_limn.resample('D', how='sum', label='right')
    logger.debug('daily_country_counts_limn: %s', daily_country_counts_limn)
    daily_country_counts_limn = daily_country_counts_limn.rename(columns=COUNTRY_NAMES)
    daily_country_counts_limn_full = copy.deepcopy(daily_country_counts_limn)
    daily_country_counts_limn = daily_country_counts_limn#[:-1]
    daily_country_source = limnpy.DataSource(limn_id='daily_mobile_wp_views_by_country',
                                             limn_name='Daily Mobile WP Views By Country',
                                             data=daily_country_counts_limn,
                                             limn_group=LIMN_GROUP)
    daily_country_source.write(basedir)
    #logger.debug('daily_country_source: %s', daily_country_source)

    monthly_country_counts = daily_country_counts_limn_full.resample(rule='M', how='sum', label='right')
    monthly_country_counts = monthly_country_counts#[:-1]
    monthly_country_source = limnpy.DataSource(limn_id='monthly_mobile_wp_views_by_country',
                                               limn_name='Monthly Mobile WP Views By Country',
                                               data=monthly_country_counts,
                                               limn_group=LIMN_GROUP)
    monthly_country_source.write(basedir)
    return daily_country_source, monthly_country_source
    

def make_zero_sources(counts, provider, provider_metadata, basedir):
    

    # logger.debug('counts:\n%s', counts)
    # logger.debug('counts.columns: %s', counts.columns)

    prov_counts = counts[counts.provider == provider]
    start_date = provider_metadata.ix[provider, 'Start Date']
    if not start_date or (isinstance(start_date, float) and math.isnan(start_date)):
        # this means that the provider isn't yet live
        if len(prov_counts['date']):
            logger.debug('prov_counts.date.min(): %s', prov_counts.date.min())
            start_date = prov_counts.date.min()
        else:
            logger.warning('provider counts for provider %s is empty', provider)
    prov_counts = prov_counts[prov_counts['date'] > start_date]
    
    if len(prov_counts) == 0:
        logger.warning('skipping provider: %s--graphs will not be available on dashbaord', provider)
        raise ValueError('provider %s is not present in carrier counts' % provider)

    # munge counts into right format
    daily_version = prov_counts.groupby(['date', 'site'], as_index=False).sum()
    daily_version_limn = daily_version.pivot('date', 'site', 'count')
    daily_version_limn = daily_version_limn.rename(columns=VERSIONS)
    daily_version_limn_full = copy.deepcopy(daily_version_limn)
    daily_version_limn = daily_version_limn.resample(rule='D', how='sum', label='right')#[:-1]
    daily_limn_name = '%s Daily Wikipedia Page Requests By Version' % title(provider)
    daily_version_source = limnpy.DataSource(limn_id=slugify(daily_limn_name),
                                             limn_name=daily_limn_name, 
                                             data=daily_version_limn,
                                             limn_group=LIMN_GROUP)
    daily_version_source.write(basedir)

    monthly_version = daily_version_limn_full.resample(rule='M', how='sum', label='right')
    monthly_version = monthly_version#[:-1]
    monthly_version_source = limnpy.DataSource(limn_id='%s_monthly_wp_views_by_version' % slugify(provider),
                                               limn_name='%s Monthly WP View By Version' % title(provider),
                                               data=monthly_version.reset_index(),
                                               limn_group=LIMN_GROUP)
    monthly_version_source.write(basedir)
    logger.debug('daily_version_source.data.columns: %s', list(daily_version_source.data.columns))
    return daily_version_source, monthly_version_source



def make_percent_sources(provider, 
                          daily_country_source, 
                          monthly_country_source, 
                          daily_version_source, 
                          monthly_version_source,
                          basedir):

    cc = PROVIDER_COUNTRY_CODES[provider]
    country = COUNTRY_NAMES[cc]
    available_versions = list(set([VERSIONS['M'], VERSIONS['Z']]) & set(daily_version_source.data.columns))
    #logger.debug('available_versions: %s', available_versions)
    #logger.debug('len(daily_version_source)=%d', len(daily_version_source.data))
    #logger.debug('len(daily_country_source)=%d', len(daily_country_source.data))

    daily_percent_df = daily_version_source.data[available_versions].sum(axis=1)
    # doesn't work because it is usually log files which are missing which don't match to timestamp days
    # daily_percent_df = daily_percent_df.ix[daily_country_source.data[cc].index]
    try:
        country_series = daily_country_source.data[country]
    except KeyError:
        raise ValueError('Country name: %s not found in countries datasource' % country)
    daily_percent_df = pd.DataFrame(daily_percent_df / country_series)
    # daily_percent_df = pd.DataFrame(daily_percent_df[daily_percent_df[0] < 1])
    daily_percent_df = daily_percent_df * 100
    daily_percent_df = daily_percent_df.reset_index()
    daily_percent_df = daily_percent_df.rename(columns={'index' : 'date', 0 : 'Country Percentage Share'})
    daily_limn_name = '%s Daily Wikipedia Page Requests as Percentage Share of %s' % (title(provider), country)
    daily_percent_source = limnpy.DataSource(limn_id=slugify(daily_limn_name),
                                             limn_name=daily_limn_name,
                                             data=daily_percent_df,
                                             limn_group=LIMN_GROUP)
    daily_percent_source.write(basedir)

    # can't just aggregate daily percents--math doesn't work like that
    monthly_percent_df = monthly_version_source.data[available_versions].sum(axis=1)
    try:
        monthly_country_series = monthly_country_source.data[country]
    except KeyError:
        raise ValueError('Country name: %s not found in countries datasource' % country)
    monthly_percent_df = pd.DataFrame(monthly_percent_df / monthly_country_series)
    monthly_percent_df = monthly_percent_df * 100
    monthly_percent_df = monthly_percent_df.reset_index()
    monthly_percent_df = monthly_percent_df.rename(columns={'index' : 'date', 0 : 'Country Percentage Share'})
    monthly_limn_name = '%s Monthly Wikipedia Page Requests as Percentage Share of %s' % (title(provider), country)
    monthly_percent_source = limnpy.DataSource(limn_id=slugify(monthly_limn_name),
                                               limn_name=monthly_limn_name,
                                               data=monthly_percent_df,
                                               limn_group=LIMN_GROUP)
    monthly_percent_source.write(basedir)
    return daily_percent_source, monthly_percent_source


def make_summary_percent_graph(datasources, provider_metadata, basedir):
    """no launch date checking because this is for internal use only"""
    logger.debug('making percent summary!')
    limn_name = 'Free Mobile Page Requests as Percent of Country'
    g = limnpy.Graph(slugify(limn_name), limn_name, []) 
    for prov, ds in datasources.items():
        g.add_metric(ds, 'Country Percentage Share', label=title(prov))
    g.graph['root']['yDomain'] = (0,50)
    g.write(basedir, set_colors=False)


def make_summary_graph(datasources, provider_metadata, basedir):
    dfs = []
    for provider, datasource in datasources.items():
        start_date = provider_metadata.ix[provider, 'Start Date']
        if not start_date or (isinstance(start_date, float) and math.isnan(start_date)):
            # this means that the provider isn't yet live
            continue
        valid_df = datasource.data[datasource.data.index > start_date]
        # logger.debug('valid_df: %s', valid_df)

        free_versions = set(map(VERSIONS.get, map(unicode.strip, provider_metadata.ix[provider, 'Version'].split(','))))
        valid_df = valid_df[list(free_versions & set(valid_df.columns))]
        # logger.debug('valid_df: %s', valid_df)
        if len(valid_df) > 0:
            dfs.append(valid_df)
        
    long_fmt = pd.concat(dfs)
    # logger.debug('long_fmt: %s', long_fmt)
    long_fmt = long_fmt.reset_index()
    long_fmt = long_fmt.rename(columns={'index' : 'date'})
    final = long_fmt.groupby('date').sum()
    final['All Versions'] = final.sum(axis=1)
    final_full = copy.deepcopy(final)
    final = final#[:-1]
    # logger.debug('final: %s', final)
    total_ds = limnpy.DataSource(limn_id='free_mobile_traffic_by_version',
                                 limn_name='Free Mobile Traffic by Version',
                                 data=final,
                                 limn_group=LIMN_GROUP)
    total_ds.write(basedir)
    total_graph = total_ds.get_graph()

    total_graph.graph['desc'] = """The <a
    href="http://www.mediawiki.org/wiki/Wikipedia_Zero">Wikipedia Zero</a>
    initiative works with mobile phone operators to enable mobile access to
    wikipedia free of data charges.  Operators provide free access to either
    the <a href="http://en.m.wikipedia.org">full mobile site</a> or the <a
    href="http://en.zero.wikipedia.org">mobile site without images</a>.  
    This graph shows the total number of free page requests coming from all
    of our mobile partners for each of those versions.  We only consider the
    requests for the versions to which each operator provides free access, 
    and we only begin counting requests after the public start date for each
    operator."""

    total_graph.graph['desc'] += make_extended_legend(['M+Z', 'M', 'Z'])
    total_graph.write(basedir)

    final_monthly = final_full.resample(rule='M', how='sum', label='right')
    final_monthly = final_monthly#[:-1]
    total_ds_monthly = limnpy.DataSource(limn_id='free_mobile_traffic_by_version_monthly',
                                         limn_name='Monthly Free Mobile Traffic by Version',
                                         data=final_monthly,
                                         limn_group=LIMN_GROUP)
    total_ds_monthly.write(basedir)
    total_graph_monthly = total_ds_monthly.get_graph()
    total_graph_monthly.graph['desc'] = total_graph.graph['desc']
    total_graph_monthly.write(basedir)

def add_version_tab(provider, daily_version_source, monthly_version_source, db, basedir):
    # daily
    daily_version_graph = daily_version_source.get_graph([VERSIONS['M'], VERSIONS['Z']])
    #daily_version_graph.graph['options']['stackedGraph'] = True
    daily_version_graph.graph['desc'] = "This graph shows the number of free page requests coming from the %s network to each of the "\
     "different Wikipedia mobile sites." % title(provider)
    daily_version_graph.graph['desc'] += make_extended_legend(['M', 'Z'])
    #return daily_version_graph.write(basedir)
    #db.add_tab('Versions', [monthly_version_graph.graph['slug'], daily_version_graph.graph['slug']])

    # monthly
    monthly_version_graph = monthly_version_source.get_graph([VERSIONS['M'], VERSIONS['Z']])
    #monthly_version_graph.graph['options']['stackedGraph'] = True
    monthly_version_graph.graph['desc'] = "This graph shows the number of free page requests coming from the %s network to each of the "\
     "different Wikipedia mobile sites. This graph is aggregated by month such that the total page requests during a particular month are "\
     "plotted as the last day of that month" % title(provider)
    monthly_version_graph.graph['desc'] += make_extended_legend(['M','Z'])
    monthly_version_graph.write(basedir)
    #return monthly_version_graph

    db.add_tab('Versions', [monthly_version_graph.graph['slug']])

def add_raw_tab(provider,
        daily_version_source,
        monthly_version_source,
        daily_country_source,
        monthly_country_source,
        db,
        basedir):

    cc = PROVIDER_COUNTRY_CODES[provider]
    country = COUNTRY_NAMES[cc]

    # daily
    daily_limn_name = '%s and Total %s Wikipedia Page Requests' % (title(provider), country)
    daily_raw_graph = limnpy.Graph(slugify(daily_limn_name), daily_limn_name)
    daily_raw_graph.add_metric(daily_country_source, country)
    daily_raw_graph.add_metric(daily_version_source, 'M')
    if 'Z' in list(daily_version_source.data.columns):
        # sometimes there are no zero page views for a provider
        daily_raw_graph.add_metric(daily_version_source, 'Z')
    daily_raw_graph.graph['desc'] = "This graph compares the number of page requests from the %s network, "\
    "for each version of the Wikipedia mobile site, with the total number of page requests to the Wikipedia "\
    "mobile site from the entire country of %s." % (title(provider), country)
    daily_raw_graph.graph['desc'] +=  make_extended_legend(['M', 'Z', 'Country'], country=country, partner=provider)
    daily_raw_graph.write(basedir)

    # monthly
    monthly_limn_name = 'Monthly ' + daily_limn_name
    monthly_raw_graph = limnpy.Graph(slugify(monthly_limn_name), monthly_limn_name)
    monthly_raw_graph.add_metric(monthly_country_source, country)
    monthly_raw_graph.add_metric(monthly_version_source, 'M')
    if 'Z' in list(monthly_version_source.data.columns):
        # sometimes there are no zero page views for a provider
        monthly_raw_graph.add_metric(monthly_version_source, 'Z')
    monthly_raw_graph.graph['desc'] = "This graph compares the number of page requests from the %s network, "\
    "for each version of the Wikipedia mobile site, with the total number of page requests to the Wikipedia mobile "\
    "site from the entire country of %s. This graph is aggregated by month such that the total page requests during a particular month are "\
    "plotted as the last day of that month." % (title(provider), country)
    monthly_raw_graph.graph['desc'] +=  make_extended_legend(['M', 'Z', 'Country'], country=country, partner=provider)
    monthly_raw_graph.write(basedir)

    #db.add_tab('Country Raw', [daily_raw_graph.graph['slug'], monthly_raw_graph.graph['slug']])
    db.add_tab('Country Raw', [monthly_raw_graph.graph['slug']])

def add_percent_tab(provider,
        daily_percent_source,
        monthly_percent_source,
        db,
        basedir):

    cc = PROVIDER_COUNTRY_CODES[provider]
    country = COUNTRY_NAMES[cc]

    daily_percent_graph = daily_percent_source.get_graph()
    daily_percent_graph.graph['desc'] = "This graph shows the percentage of all page requests "\
    "to the Wikipedia mobile sites originating in %s which come from the %s network."  % (country, title(provider))
    daily_percent_graph.write(basedir)

    monthly_percent_graph = monthly_percent_source.get_graph()
    monthly_percent_graph.graph['desc'] = "This graph shows the percentage of all page requests "\
    "to the Wikipedia mobile sites originating in %s which come from the %s network. "\
    "This graph is aggregated by month such that the total page requests during a particular month are "\
    "plotted as the last day of that month."  % (country, title(provider))
    monthly_percent_graph.write(basedir)

    #db.add_tab('Country Percent', [daily_percent_graph.graph['slug'], monthly_percent_graph.graph['slug']])
    db.add_tab('Country Percent', [monthly_percent_graph.graph['slug']])

def make_dashboard(carrier_counts, 
                   daily_country_source, 
                   monthly_country_source,
                   provider_metadata,
                   basedir,
                   provider):
    """
    Create dashbaord file and generate provider specific datasources and graphs
    """
    name = '%s Wikipedia Zero Dashboard' % title(provider)
    db = limnpy.Dashboard(slugify(provider, '-'), name, headline=title(provider), subhead='Wikipedia Zero Dashboard')

    # make provider-specific sourcess
    daily_version_source, monthly_version_source = make_zero_sources(
            carrier_counts, provider, provider_metadata, basedir)
    daily_percent_source, monthly_percent_source = make_percent_sources(
            provider, daily_country_source, monthly_country_source,
            daily_version_source, monthly_version_source, basedir)

    # make graphs and add tabs to dashboard
    add_version_tab(provider, daily_version_source, monthly_version_source, db,
            basedir)
    add_raw_tab(provider, daily_version_source, monthly_version_source,
            daily_country_source, monthly_country_source, db, basedir)
    add_percent_tab(provider, daily_percent_source, monthly_percent_source, db,
            basedir)
    db.write(basedir)
    return daily_version_source, daily_percent_source



def parse_args():
    parser = argparse.ArgumentParser(description='Process a collection of \
    squid logs and write certain extracted metrics to file')
    parser.add_argument('-l',
                        '--loglevel',
                        dest='log_level',
                        choices=LEVELS.keys(),
                        default='DEBUG',
                        help='log level')
    parser.add_argument('--zero_counts',
                        default='/a/erosen/zero_carrier_country/carrier',
                        help='file in which to find counts of zero \
                        filtered request.  each run just appends to this file.')
    parser.add_argument('--country_counts',
                        default='/a/erosen/zero_carrier_country/country',
                        help='directory containing hadoop output files which \
                        contain counts for each country, language, project and version')
    parser.add_argument('--metadata',
                        default='WP Zero Partner - Versions',
                        help='Google Drive spreadsheet title which shows the launch date for each provider')
    parser.add_argument('--providers',
                        nargs='+',
                        default=DEFAULT_PROVIDERS,
                        help='list of providers for which to create dashboards')
    parser.add_argument('--limn_basedir',
                        default='data',
                        help='basedir in which to place limn datasource/datafile/graphs/dashboards directories')
    parser.add_argument('--debug', default=False, action='store_true', help='limits to loading only 2 files from zero/sampled-cache')

    args = parser.parse_args()
    opts = vars(args)
    logger.setLevel(LEVELS.get(opts['log_level']))
    logger.info('\n' + pprint.pformat(opts))
    return opts


def main():
    opts = parse_args()
    provider_metadata = get_provider_metadata(usecache=True)
    zero_counts = load_counts(opts['zero_counts'])
    country_counts = load_counts(opts['country_counts'])

    provider_version_sources = {}
    provider_percent_sources = {}
    daily_country_source, monthly_country_source = make_country_sources(country_counts, opts['limn_basedir'])
    # make provider-specific dashboards
    for provider in opts['providers']:
        logger.info('building dashboard for provider %s', provider)
        try:
            version_source, version_percent_source = make_dashboard(
                                                        zero_counts, 
                                                        daily_country_source,
                                                        monthly_country_source,
                                                        provider_metadata, 
                                                        opts['limn_basedir'], 
                                                        provider)
        except ValueError:
            logging.exception('exception raised while constructing dashboard for %s', provider)
            continue
        provider_version_sources[provider] = version_source
        provider_percent_sources[provider] = version_percent_source

    # make summary graphs
    make_summary_graph(provider_version_sources, provider_metadata, opts['limn_basedir'])
    make_summary_percent_graph(provider_percent_sources, provider_metadata, opts['limn_basedir'])

if __name__ == '__main__':
    main()
