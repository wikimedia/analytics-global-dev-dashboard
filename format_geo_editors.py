import argparse
import logging
import json, pprint
import datetime, dateutil.parser
from collections import defaultdict, OrderedDict, Container
import itertools
from operator import itemgetter
import re
import os

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


def get_rows(json_all):
    world_rows = []
    json_tree = defaultdict(dict)
    for json_f in json_all:
        for cohort, count in json_f['world'].items():
            world_row = {'date' : json_f['end'], 'project' : json_f['project'], 'world' : True, 'cohort' :  cohort, 'count' : count}
            world_rows.append(world_row)
        json_tree[json_f['end']][json_f['project']] = json_f['countries']
    # logging.debug('f: %s' % (json.dumps(json_all, indent=2)))
    # expand tree structure of dictionaries into list of dicts with named fields
    rows = list(flatten(json_tree, [], ['date', 'project', 'country', 'cohort', 'count']))
    all_rows = rows + world_rows
    return Collection(all_rows)


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
        logging.info('creating index for key: %s', key)
        idx = defaultdict(set)
        for row_hash, row in self.row_hashes.items():
            if key in row:
                idx[row[key]].add(row_hash)
        self.indices[key] = idx
        logging.info('finished creating index for key: %s', key)

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
                candidates = reduce(set.__ior__, map(idx.get, raw_val), set())
                filtered = filtered & candidates
            #logging.debug('len(filtered): %s', len(filtered))
        rows = map(self.row_hashes.get, filtered)
        #logging.debug('probe: %s\tlen(rows): %s', pprint.pformat(probe), len(rows))
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


def make_limn_rows(rows, col_prim_key):
    transformed = []
    logging.debug('transforming rows to {\'date\' : date, \'%s (cohort)\' : count}', col_prim_key)
    for row in rows:
        if col_prim_key in row:
            transformed.append({'date' : row['date'], '%s (%s)' % (row[col_prim_key], row['cohort']) : row['count']})
    transformed = Collection(transformed)
    limn_rows = []
    dates = transformed.distinct('date')
    logging.debug('making final rows')
    for date in dates:
        limn_row = {'date' : date}
        date_rows = transformed.find({'date' : date})
        for date_row in date_rows:
            limn_row.update(date_row)
        limn_rows.append(limn_row)
    logging.debug('exiting')
    return limn_rows
        

def write_project(proj, rows, basedir):
    logging.debug('writing project datasource for: %s', proj)
    _id = proj + '_all'
    name = '%s Editors by Country' % proj.upper()

    proj_rows = rows.find({'project' : proj})
    logging.debug('len(proj_rows): %d', len(proj_rows))
    limn_rows = make_limn_rows(proj_rows, 'country')
    limnpy.write(_id, name, limn_rows, basedir=basedir)


def top_k_countries(rows, k, probe):
    filtered_rows = Collection(rows.find(probe))
    country_rows = merge_rows(['country'], filtered_rows)
    country_totals = dict(map(itemgetter('country', 'count'), country_rows))
    #logging.debug(sorted(map(list,map(reversed,country_totals.items())), reverse=True))
    keep_countries = zip(*sorted(map(list,map(reversed,country_totals.items())), reverse=True))[1][:k]
    return keep_countries


def write_project_top_k(proj, rows, basedir, k=10):
    _id = proj + '_top%d' % k
    name = '%s Editors by Country (top %d)' % (proj.upper(), k)
    top_k = top_k_countries(rows, k, {'project' : proj, 'cohort' : 'all'})
    proj_rows = rows.find({'country' : top_k, 'project' : proj})
    limn_rows = make_limn_rows(proj_rows, 'country')
    limnpy.write(_id, name, limn_rows, basedir=basedir)


def write_overall(projects, rows, basedir):
    logging.info('writing overall datasource')
    _id = 'overall'
    name = 'Overall Editors by Language'

    overall_rows = rows.find({'world' : True})
    limn_rows = make_limn_rows(overall_rows, 'project')
    #logging.debug('overall limn_rows: %s', pprint.pformat(limn_rows))
    limnpy.write(_id, name, limn_rows, basedir=basedir)


def merge_rows(group_keys, rows, merge_key='count', merge_red_fn=int.__add__, red_init=0):
    logging.debug('merging rows by grouping on: %s', group_keys)
    logging.debug('reducing field %s with fn: %s, init_val: %s', merge_key, merge_red_fn, red_init)
    group_vals = map(rows.distinct, group_keys)
    #logging.debug('group_vals: %s', pprint.pformat(dict(zip(group_keys, group_vals))))
    merged_rows = []
    for group_val in itertools.product(*group_vals):
        group_probe = dict(zip(group_keys, group_val))
        group_rows = rows.find(group_probe)
        merged_val = reduce(merge_red_fn, map(itemgetter(merge_key), group_rows), red_init)
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


def write_group(group_key, rows, basedir):
    group_rows = merge_rows([group_key, 'cohort', 'date'], rows)
    if not group_rows:
        logging.warning('group_rows for group_key: %s is empty! (group_rows: %s)', group_key, group_rows)
    limn_rows = make_limn_rows(group_rows, group_key)
    if limn_rows:
        limnpy.write(group_key.replace(' ', '_').lower(), group_key.replace('_', ' ').title(), limn_rows, basedir=basedir)
    else:
        logging.warning('limn_rows for group_key: %s is empty! (limn_rows: %s)', group_key, limn_rows)

def parse_args():

    parser = argparse.ArgumentParser(description='Format a collection of json files output by editor-geocoding and creates a single csv in digraph format.')
    parser.add_argument(
        'geo_files', 
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

    args = parser.parse_args()
    logging.info(pprint.pformat(vars(args), indent=2))
    return args

if __name__ == '__main__':
    args = parse_args()
    files = load_json_files(args.geo_files)
    projects = list(set(map(itemgetter('project'), files)))
    rows = get_rows(files)
    for project in projects:
        write_project(project, rows, args.basedir)
        write_project_top_k(project, rows, args.basedir, k=args.k)
    write_overall(projects, rows, args.basedir)

    # use metadata from Google Drive doc which lets us group by country
    country_data = gcat.get_file('Global South and Region Classifications', usecache=True)
    #logging.debug('country_data: %s', pprint.pformat(country_data))
    country_data = Collection(country_data)
    joined_rows = Collection(join('country', country_data, rows))

    write_group('country', joined_rows, args.basedir)
    write_group('region', joined_rows, args.basedir)
    write_group('global_south', joined_rows, args.basedir)
    write_group('catalyst', joined_rows, args.basedir)
