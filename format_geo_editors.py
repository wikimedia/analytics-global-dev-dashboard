import argparse
import logging
import json, pprint
import datetime, dateutil.parser
from collections import defaultdict, OrderedDict, Container
import itertools
import operator
from operator import itemgetter
import re
import os
#import MySQLdb as sql
import sqlite3 as sql
import MySQLdb.cursors
from profilehooks import profile
import multiprocessing
import traceback

import limnpy
import gcat

root_logger = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s]\t[%(threadName)s]\t[%(funcName)s:%(lineno)d]\t%(message)s')
ch.setFormatter(formatter)
root_logger.addHandler(ch)
root_logger.setLevel(logging.DEBUG)


def flatten(nest, path=[], keys=[]):
    #logging.debug('entering with type(nest):%s,\tpath: %s' % (type(nest), path))
    # try to use as dict
    try:
        #logging.debug('trying to use as dict')
        for k, v in nest.items():
            #logging.debug('calling flatten(%s, %s)' % (k, v))
            for row in flatten(v, path + [k], keys):
                yield row
    except AttributeError:
        #logging.debug('nest has not attribute \'items()\'')
        # try to use as list
        try:
            #logging.debug('trying to use as list')
            for elem in nest:
                for row in flatten(elem, path, keys):
                    yield row
        except TypeError:
            #logging.debug('nest object of type %s is not iterable' % (type(nest)))
            #logging.debug('reached leaf of type: %s' % (type(nest)))
            # must be a leaf, finally yield
            #logging.debug('yielding %s' % (path + [nest]))
            yield dict(zip(keys, path + [nest]))


def load_json_files(files):
    limn_fmt = '%Y/%m/%d'

    json_all = []
    for f in files:
        json_f = json.load(open(f, 'r'))
        json_f['end'] = dateutil.parser.parse(json_f['end']) 
        json_f['start'] = dateutil.parser.parse(json_f['start'])
        if (json_f['end'] - json_f['start']).days != 30:
            logging.info('skipping file: because it is not a 30 day period')
            continue
        json_f['end'] = json_f['end'].strftime(limn_fmt)
        json_f['start'] = json_f['start'].strftime(limn_fmt)
        json_all.append(json_f)
    return json_all


class Collection(object):

    def __init__(self, rows, index_keys=[]):
        self.row_hashes = {hash(frozenset(row.items())) : row for row in rows}
        self.row_hash_set = frozenset(self.row_hashes.keys())
        self.indices = {}
        for key in index_keys:
            self.index(key)

    def __iter__(self):
        return iter(self.row_hashes.values())

    def index(self, key):
        logging.debug('creating index for key: %s', key)
        idx = defaultdict(set)
        for row_hash, row in self.row_hashes.items():
            if key in row:
                idx[row[key]].add(row_hash)
        self.indices[key] = idx
        logging.debug('finished creating index for key: %s', key)

    @classmethod
    def is_iterable(cls, val):
        return isinstance(val, Container) and not isinstance(val, basestring)


    def find(self, probe):
        filtered = self.row_hash_set
        #logging.debug('initial len(filtered): %s', len(filtered))
        for key, raw_val in probe.items():
            if key not in self.indices:
                self.index(key)
            idx = self.indices[key]
            if not Collection.is_iterable(raw_val):
                filtered = filtered & idx[raw_val]
            else:
                candidates = reduce(operator.__or__, map(idx.get, raw_val), set())
                filtered = filtered & candidates
            #logging.debug('len(filtered): %s', len(filtered))
        rows = map(self.row_hashes.get, filtered)
        logging.debug('probe: %s\tlen(rows): %s', pprint.pformat(probe), len(rows))
        return rows
    
    def distinct(self, key):
        if key not in self.indices:
            self.index(key)
        return self.indices[key].keys()
            
    @classmethod
    def iter_find(cls, probe, rows):
        def filter_fn(row):
            for k, v in probe.items():
                if k not in row or row[k] not in Collection.smart_list(v):
                    return False
            return True
        return filter(filter_fn, rows)


def make_limn_rows(rows, col_prim_key, count_key = 'count'):
    if not rows:
        return
    logging.debug('making limn rows from rows with keys:%s', rows[0].keys())
    logging.debug('col_prim_key: %s', col_prim_key)
    logging.debug('len(rows): %s', len(rows))
    rows = map(dict, rows) # need real dicts, not sqlite.Rows

    filtered = filter(lambda r : r['cohort'] in ['all', '5+', '100+'], rows)

    transformed = []
    # logging.debug('transforming rows to {\'date\' : end, \'%s (cohort)\' : count}', col_prim_key)
    for row in filtered:
        if col_prim_key in row:
            transformed.append({'date' : row['end'], '%s (%s)' % (row[col_prim_key], row['cohort']) : row[count_key]})
        else:
            logging.debug('row does not contain col_prim_key (%s): %s', col_prim_key, row)

    logging.debug('len(transformed): %s', len(transformed))
    limn_rows = []
    for date, date_rows in itertools.groupby(sorted(transformed, key=itemgetter('date')), key=itemgetter('date')):
        limn_row = {'date' : date}
        for date_row in date_rows:
            limn_row.update(date_row)
        limn_rows.append(limn_row)
    return limn_rows

def write_default_graphs(source, limn_id, limn_name, basedir):
    if source:
        source_id = source['id']

        cohorts = [('all', 'all', 'All'), ('5\+', 'active', 'Active'), ('100\+', 'very_active', 'Very Active')]
        for cohort_str, cohort_id, cohort_name in cohorts:
            cols = [name for name in source['columns']['labels'] if re.match('.*%s.*' % cohort_str, name)]
            source_cols = list(itertools.product([source_id], cols))
            limnpy.writegraph(limn_id + '_' + cohort_id, cohort_name + ' ' + limn_name, [source], source_cols, basedir=basedir)



def write_project(proj, rows, basedir):
    logging.debug('writing project datasource for: %s', proj)
    limn_id = proj + '_all'
    name = '%s Editors by Country' % proj.upper()

    proj_rows = rows.find({'project' : proj})
    logging.debug('len(proj_rows): %d', len(proj_rows))
    limn_rows = make_limn_rows(proj_rows, 'country')
    limnpy.writedicts(limn_id, name, limn_rows, basedir=basedir)


def write_project_mysql(proj, cursor, basedir):
    logging.debug('writing project datasource for: %s', proj)
    limn_id = proj + '_all'
    limn_name = '%s Editors by Country' % proj.upper()

    if sql.paramstyle == 'qmark':
        query = """ SELECT * FROM erosen_geocode_active_editors_country WHERE project=?"""
        logging.debug('making query: %s', query)
    elif sql.paramstyle == 'format':
        query = """ SELECT * FROM erosen_geocode_active_editors_country WHERE project=%s"""
    cursor.execute(query, [proj])
    proj_rows = cursor.fetchall()
    
    logging.debug('len(proj_rows): %d', len(proj_rows))
    if not proj_rows and sql.paramstyle == 'format':
        logging.debug('GOT NUTHIN!: %s', query % proj)
    limn_rows = make_limn_rows(proj_rows, 'country')
    s = limnpy.writedicts(limn_id, limn_name, limn_rows, basedir=basedir)
    write_default_graphs(s, limn_id, limn_name, basedir)

def top_k_countries(rows, k, probe):
    filtered_rows = Collection(rows.find(probe))
    country_rows = merge_rows(['country'], filtered_rows)
    country_totals = dict(map(itemgetter('country', 'count'), country_rows))
    #logging.debug(sorted(map(list,map(reversed,country_totals.items())), reverse=True))
    sorted_countries = zip(*sorted(map(list,map(reversed,country_totals.items())), reverse=True))
    if sorted_countries:
        keep_countries = sorted_countries[1][:min(k, len(country_totals))]
    else:
        keep_countries = []
    return keep_countries


def write_project_top_k(proj, rows, basedir, k=10):
    limn_id = proj + '_top%d' % k
    name = '%s Editors by Country (top %d)' % (proj.upper(), k)
    top_k = top_k_countries(rows, k, {'project' : proj, 'cohort' : 'all'})
    proj_rows = rows.find({'country' : top_k, 'project' : proj})
    limn_rows = make_limn_rows(proj_rows, 'country')
    s = limnpy.writedicts(limn_id, name, limn_rows, basedir=basedir)
    write_default_graphs(s, limn_id, limn_name, basedir)


def write_project_top_k_mysql(proj, cursor,  basedir, k=10):
    limn_id = proj + '_top%d' % k
    limn_name = '%s Editors by Country (top %d)' % (proj.upper(), k)

    if sql.paramstyle == 'qmark':
        top_k_query = """SELECT DISTINCT(country)
                    FROM erosen_geocode_active_editors_country
                    WHERE project=? AND cohort='all'
                    ORDER BY count DESC LIMIT ?"""
    elif sql.paramstyle == 'format':
        top_k_query = """SELECT DISTINCT(country)
                    FROM erosen_geocode_active_editors_country
                    WHERE project=%s AND cohort='all'
                    ORDER BY count DESC LIMIT %s"""
    cursor.execute(top_k_query, (proj, k)) # mysqldb first converst all args to str
    top_k = cursor.fetchall()
    top_k_str = ','.join(map(itemgetter('country'), top_k))
    
    if sql.paramstyle == 'qmark':
        query = """ SELECT * FROM erosen_geocode_active_editors_country WHERE project=? AND country IN (?)"""
    elif sql.paramstyle == 'format':
        query = """ SELECT * FROM erosen_geocode_active_editors_country WHERE project=%s AND country IN (%s)"""
    cursor.execute(query, (proj, top_k_str))
    proj_rows = cursor.fetchall()

    limn_rows = make_limn_rows(proj_rows, 'country')
    s = limnpy.writedicts(limn_id, limn_name, limn_rows, basedir=basedir)
    write_default_graphs(s, limn_id, limn_name, basedir)


def write_overall(projects, rows, basedir):
    logging.info('writing overall datasource')
    limn_id = 'overall'
    limn_name = 'Overall Editors by Language'

    overall_rows = rows.find({'world' : True})
    limn_rows = make_limn_rows(overall_rows, 'project')
    #logging.debug('overall limn_rows: %s', pprint.pformat(limn_rows))
    s = limnpy.writedicts(limn_id, name, limn_rows, basedir=basedir)
    write_default_graphs(s, limn_id, limn_name, basedir)


def write_overall_mysql(projects, cursor, basedir):
    logging.info('writing overall datasource')
    limn_id = 'overall_by_lang'
    limn_name = 'Overall Editors by Language'

    query = """ SELECT * FROM erosen_geocode_active_editors_world"""
    cursor.execute(query) # mysqldb first converst all args to str
    overall_rows = cursor.fetchall()
    
    limn_rows = make_limn_rows(overall_rows, 'project')
    #logging.debug('overall limn_rows: %s', pprint.pformat(limn_rows))
    s = limnpy.writedicts(limn_id, limn_name, limn_rows, basedir=basedir)
    write_default_graphs(s, limn_id, limn_name, basedir)



def merge_rows(group_keys, rows, merge_key='count', merge_red_fn=operator.__add__, red_init=0):
    logging.debug('merging rows by grouping on: %s', group_keys)
    logging.debug('reducing field %s with fn: %s, init_val: %s', merge_key, merge_red_fn, red_init)
    group_vals = map(rows.distinct, group_keys)
    #logging.debug('group_vals: %s', pprint.pformat(dict(zip(group_keys, group_vals))))
    merged_rows = []
    for group_val in itertools.product(*group_vals):
        group_probe = dict(zip(group_keys, group_val))
        group_rows = rows.find(group_probe)
        merge_set = map(itemgetter(merge_key), group_rows)
        merged_val = reduce(merge_red_fn, merge_set, red_init)
        group_probe[merge_key] = merged_val
        merged_rows.append(group_probe)
    return merged_rows


def join(join_key, coll1, coll2):
    logging.debug('joining...')
    intersection = set(coll1.distinct(join_key)) & set(coll2.distinct(join_key))
    joined_rows = []
    for val in intersection:
        probe = {join_key : val}
        pairs = itertools.product(coll1.find(probe), coll2.find(probe))
        for row1, row2 in pairs:
            joined_rows.append(dict(row1.items() + row2.items()))
    logging.debug('done')
    return joined_rows


def write_group_mysql(group_key, country_data, cursor, basedir):
    country_data = filter(lambda row: group_key in row, country_data)
    country_data = sorted(country_data, key=itemgetter(group_key))
    groups = itertools.groupby(country_data, key=itemgetter(group_key))
    groups = dict(map(lambda (key, rows) : (key, map(itemgetter('country'), rows)), groups))
    #logging.debug(pprint.pformat(groups))
    all_rows = []
    for group_val, countries in groups.items():
        logging.debug('processing group_val: %s', group_val)
        if sql.paramstyle == 'qmark':
            group_query = """SELECT end, cohort, SUM(count) 
                         FROM erosen_geocode_active_editors_country
                         WHERE country IN (%s)
                         GROUP BY end, cohort"""
            countries_fmt = ', '.join([' ? ']*len(countries))
        elif sql.paramstyle == 'format':
            group_query = """SELECT end, cohort, SUM(count) 
                         FROM erosen_geocode_active_editors_country
                         WHERE country IN (%s)
                         GROUP BY end, cohort"""
        group_query_fmt = group_query % countries_fmt
        cursor.execute(group_query_fmt, tuple(countries))
        group_rows = cursor.fetchall()
        group_rows = map(dict, group_rows)
        for row in group_rows:
            row.update({group_key : group_val})
        all_rows.extend(group_rows)
    #logging.debug('groups_rows: %s', group_rows)

    limn_rows = make_limn_rows(all_rows, group_key, count_key='SUM(count)')
    limn_id = group_key.replace(' ', '_').lower()
    limn_name = group_key.title()
    s = limnpy.writedicts(limn_id, limn_name, limn_rows, basedir=basedir)
    write_default_graphs(s, limn_id, limn_name, basedir)


def write_group(group_key, rows, basedir):
    group_rows = merge_rows([group_key, 'cohort', 'date'], rows)
    if not group_rows:
        logging.warning('group_rows for group_key: %s is empty! (group_rows: %s)', group_key, group_rows)
    limn_rows = make_limn_rows(group_rows, group_key)
    if limn_rows:
        limnpy.writedicts(group_key.replace(' ', '_').lower(), group_key.replace('_', ' ').title(), limn_rows, basedir=basedir)
    else:
        logging.warning('limn_rows for group_key: %s is empty! (limn_rows: %s)', group_key, limn_rows)

def parse_args():

    parser = argparse.ArgumentParser(description='Format a collection of json files output by editor-geocoding and creates a single csv in digraph format.')
    parser.add_argument(
        '--geo_files', 
        metavar='GEOCODING_FILE.json', 
        nargs='+',
        help='any number of appropriately named json files')
    parser.add_argument(
        '-d','--basedir',
        default='.',
        type=os.path.expanduser,
        help='directory in which to find or create the datafiles and datasources directories for the *.csv and *.yaml files')
    parser.add_argument(
        '-b', '--basename',
        default='geo_editors',
        help='base file name for csv and yaml files.  for example: BASEDIR/datasources/BAS_FILENAME_en.yaml')
    parser.add_argument(
        '-k', 
        type=int, 
        default=10, 
        help='the number of countries to include in the selected project datasource')
    parser.add_argument(
        '-p', '--parallel',
        action='store_true',
        default=False,
        help='use a multiprocessing pool to execute per-language analysis in parallel'
        )

    args = parser.parse_args()
    logging.info(pprint.pformat(vars(args), indent=2))
    return args

def get_projects():
    f = open('/home/erosen/src/dashboard/data/all_ids.tsv')
    projects = []
    for line in f:
        projects.append(line.split('\t')[0].strip())
    return projects


def process_project_par((project, basedir)):
    try:
        logging.info('processing project: %s', project)
        # db = sql.connect(read_default_file=os.path.expanduser('~/.my.cnf.research'), db='staging')
        # cursor = db.cursor(MySQLdb.cursors.DictCursor)

        db = sql.connect('/home/erosen/src/editor-geocoding/geowiki.sqlite')
        write_project_mysql(project, cursor, args.basedir)
        write_project_top_k_mysql(project, cursor, args.basedir, k=args.k)
    except:
        logging.exception('caught exception in process:')
        raise    

def process_project(project, cursor, basedir):
    logging.info('processing project: %s (%d/%d)', project, i, len(projects))
    write_project_mysql(project, cursor, args.basedir)
    write_project_top_k_mysql(project, cursor, args.basedir, k=args.k)
    

if __name__ == '__main__':
    args = parse_args()

    # db = MySQLdb.connect(read_default_file=os.path.expanduser('~/.my.cnf.research'), db='staging', cursorclass=MySQLdb.cursors.DictCursor)
    db = sql.connect('/home/erosen/src/editor-geocoding/geowiki.sqlite')
    db.row_factory = sql.Row
    cursor = db.cursor()

    projects = get_projects()
    if not args.parallel or sql.threadsafety < 2:
        for i, project in enumerate(projects):
            logging.info('processing project: %s (%d/%d)', project, i, len(projects))
            process_project(project, cursor, args.basedir)
    else:
        pool = multiprocessing.Pool(10)
        pool.map_async(process_project_par, itertools.izip(projects, itertools.repeat(args.basedir))).get(99999)

    # write_overall_mysql(projects, cursor, args.basedir)

    # use metadata from Google Drive doc which lets us group by country
    country_data = gcat.get_file('Global South and Region Classifications', sheet='data', fmt='dict', usecache=True)
    logging.debug('typ(country_data): %s', type(country_data))
    logging.info('country_data[0].keys: %s', country_data[0].keys())

    write_group_mysql('country', country_data, cursor, args.basedir)
    write_group_mysql('region', country_data, cursor, args.basedir)
    write_group_mysql('global_south', country_data, cursor, args.basedir)
    write_group_mysql('catalyst', country_data, cursor, args.basedir)
