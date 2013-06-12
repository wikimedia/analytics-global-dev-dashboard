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
import json
from apiclient.discovery import HttpError

import gcat
import limnpy
import mccmnc


DEFAULT_CARRIER_IDS = ['digi-telecommunications-malaysia',
                     'orange-kenya',
                     'orange-sahelc-niger',
                     'orange-tunisia',
                     'orange-uganda',
                     'orange-botswana',
                     'orange-cameroon',
                     'orange-ivory-coast',
                     'promonte-gsm-montenegro',
                     'stc-al-jawal-saudi-arabia',
                     'tata-india',
                     'total-access-dtac-thailand',
                     'cct-congo-dem-rep',
                     'mobilink-pakistan']

LIMN_GROUP = 'gp'

LEVELS = {'DEBUG': logging.DEBUG,
          'INFO': logging.INFO,
          'WARNING': logging.WARNING,
          'ERROR': logging.ERROR,
          'CRITICAL': logging.CRITICAL}

#DATE_FMT = '%Y-%m-%d_%H'
DATE_FMT = '%Y-%m-%d'

VERSIONS = {'X' : 'X', 'M' : 'M', 'M+Z' : 'M+Z', 'Z' : 'Z', 'Country' : 'Country'}
VERSIONS_LONG = {'X' : 'Free page views from carrier to desktop (non-mobile) Wikipedia urls',
                'M' : 'Free page views from carrier to m.wikipedia.org urls',
                'Z' : 'Free page views from carrier to zero.wikipedia.org',
                'M+Z' : 'Combined free page views from carrier to m.wikipedia.org and zero.wikipedia.org urls',
                'Country' : 'Total page views within country (all networks, not just carrier) to m.wikipedia.org and zero.wikipedia.org urls'}

FIELDS = ['date', 'lang', 'project', 'site', 'country', 'carrier']
#COUNT_FIELDS = ['count'] + FIELDS
COUNT_FIELDS = FIELDS + ['count']

class Carrier(object):

    carrier_info = mccmnc.mccmnc(usecache=False)
    carrier_info.append({
        "network": "Tata", 
        "country": "India", 
        "mcc": "405", 
        "iso": "AF", 
        "country_code": "IN", 
        "mcc_mnc": "405-*", 
        "mnc": "*",
        "name" : "Tata India",
        "slug": "tata-india"
    })
    carrier_version_info = gcat.get_file('WP Zero Partner - Versions', 
            fmt='pandas', usecache=True).set_index('Slug')

    def __init__(self, slug):
        filtered = filter(lambda r : r['slug'] == slug, self.carrier_info)
        try:
            record = filtered[0]
        except IndexError:
            logger.exception('could not find carrier identified by slug: %s in carrier_info', slug)
            raise
        self.__dict__.update(record)
        
        try:
            meta_record = self.carrier_version_info.ix[self.slug,:]
        except:
            logger.exception('failed to find carrier identified by slug: %s in Google Drive version doc', slug)
            raise
        meta_record = {slugify(k) : v for k,v in dict(meta_record).items()}
        self.__dict__.update(meta_record)
        self.versions = [v.strip() for v in unicode(self.versions).split(',')]

_punct_re = re.compile(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.-]+')
def slugify(text, delim=u'_'):
    """Generates an ASCII-only slug. Credit to Armin Ronacher from
    http://flask.pocoo.org/snippets/5/"""
    result = []
    for word in _punct_re.split(text.lower()):
        result.extend(unidecode.unidecode(word).split())
    return unicode(delim.join(result))

def make_extended_legend(versions, carrier):
    table = lambda s : '<table vspace=\"20\">%s</table>' % s
    row = lambda s : '<tr>%s</tr>' % s
    cell = lambda s : '<td>%s</td>' % s
    top_right_cell = lambda s : '<td valign=\"top\" align=\"right\">%s &nbsp;&nbsp; : &nbsp;&nbsp; </td>' % s
    country_replace = lambda s : s.replace('country', carrier.country)
    carrier_replace = lambda s : s.replace('carrier', carrier.name)
    bold = lambda s : '<strong>%s  </strong>' % s
    title = lambda s : ' \n <h3>Extended Legend</h3> \n %s' % s

    rows = ''
    for v in versions:
        cells = top_right_cell(bold(VERSIONS[v])) + cell(VERSIONS_LONG[v])
        rows += row(cells)
    t = table(rows)
    final = title(t)
    final = country_replace(final)
    final = carrier_replace(final)
    return final

def load_counts(cache_dir, sep, date_fmt):
    counts = pd.DataFrame(columns=COUNT_FIELDS)
    date_col_ind = COUNT_FIELDS.index('date')

    i = 0
    for root, dirs, files in os.walk(cache_dir):
        for count_file in files:
            full_path = os.path.join(root, count_file)
            logger.debug('processing: %s', full_path)
            try:
                df = pd.read_table(
                        full_path,
                        parse_dates=[date_col_ind],
                        date_parser=lambda s : datetime.datetime.strptime(s,
                             date_fmt),
                        skiprows=1,
                        sep=sep,
                        names=COUNT_FIELDS)
                logger.debug('processing: %s', full_path)
                logger.debug('loaded %d lines from file %d: %s', len(df), i, full_path)
                counts = counts.append(df)
                i += 1
            except StopIteration: # this is what happens when Pandas tries to load an empty file
                logger.debug('skipping empty file: %s', full_path)
            except:
                logger.exception('exception caught while loading cache file: %s', count_file)

    counts = counts[counts['project'].isin(['wikipedia.org', 'wikipedia'])]
    logger.debug('loaded_counts:%s\n%s', counts, counts[:10])
    return counts


def clean_carrier_counts(carrier_counts):
    replace_dict = {
            '405-0%2A' : 'tata-india',
            '405-0*'  : 'tata-india',
            'cct-congo,-dem-rep' : 'cct-congo-dem-rep',
            'stc/al-jawal-saudi-arabia' : 'stc-al-jawal-saudi-arabia',
            'total-access-(dtac)-thailand' : 'total-access-dtac-thailand',
            'digi-malaysia' : 'digi-telecommunications-malaysia',
            'orange-niger' : 'orange-sahelc-niger',
            'orange-tunesia' : 'orange-tunisia',
            'saudi-telecom' :  'stc-al-jawal-saudi-arabia',
            'dtac-thailand' : 'total-access-dtac-thailand'
            }
    carrier_counts.carrier = carrier_counts.carrier.replace(replace_dict)
    return carrier_counts


def make_country_sources(counts, basedir, prefix):
    if not len(counts):
        logger.warning('country counts is empty, returning None')
        return None, None
    country_counts = counts.groupby(['country', 'date'], as_index=False).sum()
    country_counts_limn = country_counts.pivot('date', 'country', 'count')
    daily_country_counts_limn = country_counts_limn.resample('D', how='sum', label='right')
    logger.debug('daily_country_counts_limn: %s', daily_country_counts_limn)
    #daily_country_counts_limn = daily_country_counts_limn.rename(columns=COUNTRY_NAMES)
    daily_country_counts_limn_full = copy.deepcopy(daily_country_counts_limn)
    daily_country_counts_limn = daily_country_counts_limn#[:-1]
    daily_country_source = limnpy.DataSource(limn_id=prefix + 'daily_mobile_wp_views_by_country',
                                             limn_name='Daily Mobile WP Views By Country',
                                             data=daily_country_counts_limn,
                                             limn_group=LIMN_GROUP)
    daily_country_source.write(basedir)
    #logger.debug('daily_country_source: %s', daily_country_source)

    monthly_country_counts = daily_country_counts_limn_full.resample(rule='M', how='sum', label='right')
    monthly_country_counts = monthly_country_counts#[:-1]
    monthly_country_source = limnpy.DataSource(limn_id=prefix + 'monthly_mobile_wp_views_by_country',
                                               limn_name='Monthly Mobile WP Views By Country',
                                               data=monthly_country_counts,
                                               limn_group=LIMN_GROUP)
    monthly_country_source.write(basedir)
    return daily_country_source, monthly_country_source


def make_version_sources(counts, carrier, basedir, prefix):
    # logger.debug('counts:\n%s', counts)
    # logger.debug('counts.columns: %s', counts.columns)

    prov_counts = counts[counts.carrier == carrier.slug]
    start_date = carrier.start_date
    if not start_date or (isinstance(start_date, float) and math.isnan(start_date)):
        # this means that the carrier isn't yet live
        if len(prov_counts['date']):
            logger.debug('prov_counts.date.min(): %s', prov_counts.date.min())
            start_date = prov_counts.date.min()
        else:
            logger.warning('carrier counts for carrier %s is empty', carrier.slug)
    prov_counts = prov_counts[prov_counts['date'] > start_date]
    
    if len(prov_counts) == 0:
        logger.warning('skipping carrier: %s--graphs will not be available on dashbaord', carrier.name)
        raise ValueError('carrier %s is not present in carrier counts' % carrier.name)

    # munge counts into right format
    daily_version = prov_counts.groupby(['date', 'site'], as_index=False).sum()
    daily_version_limn = daily_version.pivot('date', 'site', 'count')
    daily_version_limn = daily_version_limn.rename(columns=VERSIONS)
    daily_version_limn_full = copy.deepcopy(daily_version_limn)
    daily_version_limn = daily_version_limn.resample(rule='D', how='sum', label='right')#[:-1]
    daily_limn_name = '%s Daily Wikipedia Page Requests By Version' % carrier.name
    daily_version_source = limnpy.DataSource(limn_id=slugify(prefix + daily_limn_name),
                                             limn_name=daily_limn_name, 
                                             data=daily_version_limn,
                                             limn_group=LIMN_GROUP)
    daily_version_source.write(basedir)

    monthly_version = daily_version_limn_full.resample(rule='M', how='sum', label='right')
    monthly_version = monthly_version#[:-1]
    monthly_limn_name = '%s Monthly WP View By Version' % carrier.name
    monthly_version_source = limnpy.DataSource(limn_id=slugify(prefix + monthly_limn_name),
                                               limn_name=monthly_limn_name,
                                               data=monthly_version.reset_index(),
                                               limn_group=LIMN_GROUP)
    monthly_version_source.write(basedir)
    logger.debug('daily_version_source.data.columns: %s', list(daily_version_source.data.columns))
    return daily_version_source, monthly_version_source



def make_percent_sources(carrier, 
                          daily_country_source, 
                          monthly_country_source, 
                          daily_version_source, 
                          monthly_version_source,
                          basedir,
                          prefix):

    available_versions = list(set([VERSIONS['M'], VERSIONS['Z']]) & set(daily_version_source.data.columns))
    #logger.debug('available_versions: %s', available_versions)
    #logger.debug('len(daily_version_source)=%d', len(daily_version_source.data))
    #logger.debug('len(daily_country_source)=%d', len(daily_country_source.data))

    daily_percent_df = daily_version_source.data[available_versions].sum(axis=1)
    # doesn't work because it is usually log files which are missing which don't match to timestamp days
    try:
        country_series = daily_country_source.data[carrier.iso]
    except KeyError:
        raise ValueError('Country name: %s not found in countries datasource' % carrier.country)
    daily_percent_df = pd.DataFrame(daily_percent_df / country_series)
    # daily_percent_df = pd.DataFrame(daily_percent_df[daily_percent_df[0] < 1])
    daily_percent_df = daily_percent_df * 100
    daily_percent_df = daily_percent_df.reset_index()
    daily_percent_df = daily_percent_df.rename(columns={'index' : 'date', 0 : 'Country Percentage Share'})
    daily_limn_name = '%s Daily Wikipedia Page Requests as Percentage Share of %s' % (carrier.name, carrier.country)
    daily_percent_source = limnpy.DataSource(limn_id=slugify(prefix + daily_limn_name),
                                             limn_name=daily_limn_name,
                                             data=daily_percent_df,
                                             limn_group=LIMN_GROUP)
    daily_percent_source.write(basedir)

    # can't just aggregate daily percents--math doesn't work like that
    monthly_percent_df = monthly_version_source.data[available_versions].sum(axis=1)
    try:
        monthly_country_series = monthly_country_source.data[carrier.iso]
    except KeyError:
        raise ValueError('Country name: %s not found in countries datasource' % carrier.country)
    monthly_percent_df = pd.DataFrame(monthly_percent_df / monthly_country_series)
    monthly_percent_df = monthly_percent_df * 100
    monthly_percent_df = monthly_percent_df.reset_index()
    monthly_percent_df = monthly_percent_df.rename(columns={'index' : 'date', 0 : 'Country Percentage Share'})
    monthly_limn_name = '%s Monthly Wikipedia Page Requests as Percentage Share of %s' % (carrier.name, carrier.country)
    monthly_percent_source = limnpy.DataSource(limn_id=slugify(prefix + monthly_limn_name),
                                               limn_name=monthly_limn_name,
                                               data=monthly_percent_df,
                                               limn_group=LIMN_GROUP)
    monthly_percent_source.write(basedir)
    return daily_percent_source, monthly_percent_source


def make_summary_percent_graph(datasources, basedir, prefix):
    """no launch date checking because this is for internal use only"""
    logger.debug('making percent summary!')
    limn_name = 'Free Mobile Page Requests as Percent of Country'
    g = limnpy.Graph(slugify(prefix + limn_name), limn_name, []) 
    for prov, ds in datasources.items():
        g.add_metric(ds, 'Country Percentage Share', label=prov.name)
    g.graph['root']['yDomain'] = (0,50)
    g.write(basedir, set_colors=False)


def make_summary_version_graph(datasources, basedir, prefix):
    dfs = []
    for carrier, datasource in datasources.items():
        start_date = carrier.start_date
        if not start_date or (isinstance(start_date, float) and math.isnan(start_date)):
            # this means that the carrier isn't yet live
            continue
        valid_df = datasource.data[datasource.data.index > start_date]
        # logger.debug('valid_df: %s', valid_df)

        free_versions = set(map(VERSIONS.get, carrier.versions))
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
    total_ds = limnpy.DataSource(limn_id=prefix + 'free_mobile_traffic_by_version',
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
    of our mobile carriers for each of those versions.  We only consider the
    requests for the versions to which each operator provides free access, 
    and we only begin counting requests after the public start date for each
    operator."""

    #total_graph.graph['desc'] += make_extended_legend(['M+Z', 'M', 'Z'])
    total_graph.write(basedir)

    final_monthly = final_full.resample(rule='M', how='sum', label='right')
    final_monthly = final_monthly#[:-1]
    total_ds_monthly = limnpy.DataSource(limn_id=prefix + 'free_mobile_traffic_by_version_monthly',
                                         limn_name='Monthly Free Mobile Traffic by Version',
                                         data=final_monthly,
                                         limn_group=LIMN_GROUP)
    total_ds_monthly.write(basedir)
    total_graph_monthly = total_ds_monthly.get_graph()
    total_graph_monthly.graph['desc'] = total_graph.graph['desc']
    total_graph_monthly.write(basedir)


def make_version_graph(carrier, version_source, basedir, daily=False):
    version_graph = version_source.get_graph([VERSIONS['M'], VERSIONS['Z']])
    version_graph.graph['desc'] = "This graph shows the number of free page "\
            "requests coming from the %s network to each of the different "\
            "Wikipedia mobile sites." % carrier.name
    if not daily:
        version_graph.graph['desc'] += "  This graph is aggregated by month such "\
                "that the total page requests during a particular month are "\
                "plotted as the last day of that month"
    version_graph.graph['desc'] += make_extended_legend(['M', 'Z'], carrier)
    version_graph.write(basedir) 
    return version_graph


def make_raw_graph(carrier, version_source, country_source, basedir, prefix, daily=False):
    limn_name = '%s and Total %s Wikipedia Page Requests' % (carrier.name, carrier.country)
    if daily:
        limn_name = 'Daily ' + limn_name
    else:
        limn_name = 'Monthly ' + limn_name
    raw_graph = limnpy.Graph(slugify(prefix + limn_name), limn_name)
    raw_graph.add_metric(country_source, carrier.iso)
    raw_graph.add_metric(version_source, 'M')
    if 'Z' in list(version_source.data.columns):
        # sometimes there are no zero page views for a carrier
        raw_graph.add_metric(version_source, 'Z')
    raw_graph.graph['desc'] = "This graph compares the number of page requests from the %s network, "\
    "for each version of the Wikipedia mobile site, with the total number of page requests to the Wikipedia "\
    "mobile site from the entire country of %s." % (carrier.name, carrier.country)
    if not daily:
        raw_graph.graph['desc'] += "  This graph is aggregated by month such "\
                "that the total page requests during a particular month are "\
                "plotted as the last day of that month"
    raw_graph.graph['desc'] +=  make_extended_legend(['M', 'Z', 'Country'], carrier)
    raw_graph.write(basedir)
    return raw_graph


def make_percent_graph(carrier, percent_source, basedir, daily=False):
    percent_graph = percent_source.get_graph()
    percent_graph.graph['desc'] = "This graph shows the percentage of all page requests "\
    "to the Wikipedia mobile sites originating in %s which come from the %s network."  % (carrier.country, carrier.name)
    if not daily:
        percent_graph.graph['desc'] += "  This graph is aggregated by month such "\
                "that the total page requests during a particular month are "\
                "plotted as the last day of that month"
    percent_graph.write(basedir)
    return percent_graph

def make_dashboard(carrier_counts, 
                   daily_country_source, 
                   monthly_country_source,
                   basedir,
                   carrier,
                   prefix,
                   daily=False):
    """
    Create dashbaord file and generate carrier specific datasources and graphs
    """
    name = '%s Wikipedia Zero Dashboard' % carrier.name
    db = limnpy.Dashboard(slugify(prefix + carrier.name, '-'),
            name, headline=carrier.name, subhead='Wikipedia Zero Dashboard')

    # make carrier-specific sources and graphs
    daily_version_source, monthly_version_source = make_version_sources(
            carrier_counts, carrier, basedir, prefix)
    version_graphs = [make_version_graph(carrier, monthly_version_source, basedir)]
    daily_version_graph = make_version_graph(carrier, daily_version_source, basedir, daily=True)
    if daily:
        version_graphs.append(daily_version_graph)
    db.add_tab('Versions', version_graphs)
   
    # this let's use make dashboards without country counts if we so please
    if daily_country_source is not None and monthly_country_source is not None:
        daily_percent_source, monthly_percent_source = make_percent_sources(
                carrier, daily_country_source, monthly_country_source,
                daily_version_source, monthly_version_source, basedir, prefix)

        # make graphs and add tabs to dashboard
        raw_graphs     = [make_raw_graph(carrier, monthly_version_source, monthly_country_source, basedir, prefix)]
        percent_graphs = [make_percent_graph(carrier, monthly_percent_source, basedir)]

        # always make daily graphs
        daily_raw_graph     = make_raw_graph(carrier, daily_version_source, daily_country_source, basedir, prefix, daily=True)
        daily_percent_graph = make_percent_graph(carrier, daily_percent_source, basedir, daily=True)

        if daily:
            raw_graphs.append(daily_raw_graph)
            percent_graphs.append(daily_percent_graph)

        db.add_tab('Raw', raw_graphs)
        db.add_tab('Percent', percent_graphs)
    else:
        daily_percent_source = None

    db.write(basedir)

    # return carrier-specific sources for use in summary graphs
    return daily_version_source, daily_percent_source


def main(opts):
    carrier_counts = load_counts(opts['carrier_counts'],
            sep=opts['carrier_sep'], date_fmt=opts['carrier_date_fmt'])
    carrier_counts = clean_carrier_counts(carrier_counts)
    logger.info('carrier_counts.carrier.unique():\n%s', '\n'.join(map(str, sorted(carrier_counts.carrier.unique()))))
    country_counts = load_counts(opts['country_counts'],
            sep=opts['country_sep'], date_fmt=opts['country_date_fmt'])

    carrier_version_sources = {}
    carrier_percent_sources = {}
    daily_country_source, monthly_country_source = make_country_sources(country_counts, opts['limn_basedir'], opts['prefix'])
    # make carrier-specific dashboards
    for carrier_slug in opts['carriers']:
        carrier = Carrier(carrier_slug)
        logger.info('building dashboard for carrier %s', carrier.slug)
        try:
            version_source, version_percent_source = make_dashboard(
                                                        carrier_counts, 
                                                        daily_country_source,
                                                        monthly_country_source,
                                                        opts['limn_basedir'], 
                                                        carrier,
                                                        prefix=opts['prefix'],
                                                        daily=opts['daily'])
        except ValueError:
            logging.exception('exception raised while constructing dashboard for %s', carrier.slug)
            continue
        carrier_version_sources[carrier] = version_source
        carrier_percent_sources[carrier] = version_percent_source

    # make summary graphs
    make_summary_version_graph(carrier_version_sources, opts['limn_basedir'], prefix=opts['prefix'])
    if daily_country_source is not None and monthly_country_source is not None:
        make_summary_percent_graph(carrier_percent_sources, opts['limn_basedir'], prefix=opts['prefix'])
    logger.info('created dashboards:\n%s', '\n'.join(['gp.wmflabs.org/dashboards/%s-%s' % (slugify(opts['prefix'], '-'), c.slug) for c in carrier_version_sources.keys()]))


def parse_args():
    parser = argparse.ArgumentParser(description='Process a collection of \
    squid logs and write certain extracted metrics to file')
    parser.add_argument('-l',
                        '--loglevel',
                        dest='log_level',
                        choices=LEVELS.keys(),
                        default='DEBUG',
                        help='log level')
    parser.add_argument('--carrier_counts',
                        default='carrier',
                        help='file in which to find counts of zero \
                        filtered request.  each run just appends to this file.')
    parser.add_argument('--country_counts',
                        default='country',
                        help='directory containing hadoop output files which \
                        contain counts for each country, language, project and version')
    parser.add_argument('--prefix', default = '', help='string to prepend to all graph and dashboard names')
    parser.add_argument('--carrier_sep', default='\t')
    parser.add_argument('--country_sep', default='\t')
    parser.add_argument('--carrier_date_fmt', default=DATE_FMT)
    parser.add_argument('--country_date_fmt', default=DATE_FMT)
    parser.add_argument('--metadata',
                        default='WP Zero Partner - Versions',
                        help='Google Drive spreadsheet title which shows the launch date for each carrier')
    parser.add_argument('--carriers',
                        nargs='+',
                        default=DEFAULT_CARRIER_IDS,
                        help='list of carriers for which to create dashboards')
    parser.add_argument('--limn_basedir',
                        default='data',
                        help='basedir in which to place limn datasource/datafile/graphs/dashboards directories')
    parser.add_argument('--daily', default=False, action='store_true', help='whether to include daily graphs on dashboards')

    args = parser.parse_args()
    opts = vars(args)
    logger.setLevel(LEVELS.get(opts['log_level']))
    logger.info('\n' + pprint.pformat(opts))
    return opts


if __name__ == '__main__':
    opts = parse_args()
    main(opts)
