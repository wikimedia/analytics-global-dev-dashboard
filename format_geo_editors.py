import argparse
import logging as log
import json
from datetime import datetime
from collections import defaultdict
from functools import partial
from nesting import Nest
from operator import itemgetter
import yaml
import re
import os

root_logger = log.getLogger()
ch = log.StreamHandler()
formatter = log.Formatter('[%(levelname)s]\t[%(threadName)s]\t[%(funcName)s:%(lineno)d]\t%(message)s')
ch.setFormatter(formatter)
root_logger.addHandler(ch)
root_logger.setLevel(log.DEBUG)

def get_date(fname):
    dstr = fname.split('_')[1]
    full_fmt = '%Y%m%d'
    monthly_fmt = '%Y%m'
    try:
        d = datetime.strptime(dstr, full_fmt)
    except ValueError:
        d = datetime.strptime(dstr, monthly_fmt)
    limn_fmt = '%Y/%m/%d'
    return d.date().strftime(limn_fmt)


def merge_elems(row, start=0, end=-1, fmt=None):
    # log.debug('entering with start=%d, end=%d, fmt=%s', start, end, fmt)
    # log.debug('init row: %s' % row)
    # log.debug('to merge: %s' % (row[start:end]))
    if not fmt:
        row[start] = ''.join(row[start:end])
    else:
        row[start] = fmt % tuple(row[start:end])
    del row[start+1:end]
    # log.debug('final row: %s' % row)
    return row

def merge_items(row, keys=[], new_key=None, fmt=None):
    # log.debug('entering with keys=%s, new_key=%s, fmt=%s', keys, new_key, fmt)
    # log.debug('init row: %s' % row)
    # log.debug('to merge: %s' % [row[k] for k in keys])
    if not set(keys).issubset(set(row.keys())):
        return row
    if not fmt:
        row[new_key] = reduce(lambda k1, k2: '%s:%s' % (k1,row[k2]), keys)
    else:
        row[new_key] = fmt % tuple([str(row[k]) for k in keys])
    for k in keys:
        del row[k]
    # log.debug('final row: %s' % row)
    return row


def dfs(nest, path, keys=None):
    #log.debug('entering with type(nest):%s,\tpath: %s' % (type(nest), path))
    # try to use as dict
    try:
        #log.debug('trying to use as dict')
        for k, v in nest.items():
            #log.debug('calling dfs(%s, %s)' % (k, v))
            for row in dfs(v, path + [k], keys):
                yield row
    except AttributeError:
        #log.debug('nest has not attribute \'items()\'')
        # try to use as list
        try:
            #log.debug('trying to use as list')
            for elem in nest:
                for row in dfs(elem, path, keys):
                    yield row
        except TypeError:
            #log.debug('nest object of type %s is not iterable' % (type(nest)))
            #log.debug('reached leaf of type: %s' % (type(nest)))
            # must be a leaf, finally yield
            #log.debug('yielding %s' % (path + [nest]))
            yield dict(zip(keys, path + [nest]))


def load_json_files(files):
    json_all = defaultdict(dict)
    for f in files:
        json_f = json.load(open(f, 'r'))
        json_all[get_date(f)][json_f['project']] = json_f['countries']
    # log.debug('f: %s' % (json.dumps(json_all, indent=2)))
    # expand tree structure of dictionaries into row structure with named fields
    rows = dfs(json_all, [], ['date', 'project', 'country', 'cohort', 'count'])
    # merge two fields together by concatenating field names
    country_cohort_merger = partial(merge_items, keys=['country','cohort'], new_key='country/cohort', fmt='%s (%s)')
    merged_country_cohort = map(country_cohort_merger, rows)
    #log.debug(json.dumps(merged_country_cohort, indent=2))
    counts_by_country_cohort_by_date_by_proj = Nest()\
        .key(itemgetter('project'))\
        .key(itemgetter('date'))\
        .key(itemgetter('country/cohort'))\
        .rollup(lambda d: d[0]['count'])\
        .map(merged_country_cohort)
    #log.debug('counts_by_country_cohort_by_date_by_proj: %s' % (json.dumps(counts_by_country_cohort_by_date_by_proj, indent=2)))
    return counts_by_country_cohort_by_date_by_proj


def write_datasources(json_all, args):
    for proj, rows in json_all.items():
        write_datasource(proj, rows, args)

def write_yaml(proj, rows, all_fields, csv_name, args):

    meta = {}
    meta['id'] = 'active_editors_' + proj
    meta['name'] = proj.upper() + ' Editors'
    meta['shortName'] = meta['name']
    meta['format'] = 'csv'
    meta['url'] = '/data/datafiles/' + csv_name

    timespan = {}
    timespan['start'] = sorted(rows.keys())[0]
    timespan['end'] = sorted(rows.keys())[-1]
    timespan['step'] = '1mo'
    meta['timespan'] = timespan

    columns = {}
    columns['types'] = ['date'] + ['int' for key in all_fields]
    labels = ['Date'] + all_fields
    clean_labels = []
    for label in labels:
        tmp = re.sub('\s', '_', label)
        tmp = re.sub('[\(\)]', '', tmp)
        clean_labels.append(tmp)
    columns['labels'] = clean_labels
    meta['columns'] = columns

    meta['chart'] = {'chartType' : 'dygraphs'}

    fyaml = open(args.datasource_dir + os.sep + args.outfile + '_' + proj + '.yaml', 'w')
    fyaml.write(yaml.safe_dump(meta, default_flow_style=False))
    fyaml.close()



def write_datasource(proj, rows, args):
    log.debug('proj: %s,\tlen(rows): %s' % (proj, len(rows)))
    csv_name = args.outfile + '_' + proj + '.csv'
    csv_path = args.datafile_dir + os.sep + csv_name
    csv = open(csv_path, 'w')

    # normalize fields
    all_fields = sorted(reduce(set.__ior__, map(lambda row : set(row.keys()), rows.values()), set()))
    #log.debug('len(all_fields)=%d, all_fields:\n%s' % (len(all_fields), '\n'.join(sorted(all_fields))))

    write_yaml(proj, rows, all_fields, csv_name, args)

    csv.write(','.join(['Date'] + all_fields) + '\n')
    for date, row in sorted([item for item in rows.items()]):
        #log.debug('date: %s,\trow: %s' % (date, row))
        #log.debug('len(row): %d' % (len(row)))
        normalized_row = [row.get(key, 0) for key in all_fields]
        line = ','.join(map(str,[date] + normalized_row)) + '\n'
        # log.debug('line: %s' % (line))
        csv.write(line)
    csv.close() 

def parse_args():

    parser = argparse.ArgumentParser(description='Format a collection of json files output by editor-geocoding and creates a single csv in digraph format.')
    parser.add_argument('geo_files', metavar='GEOCODING_FILE.json', type=str, nargs='+', help='any number of appropriately named json files')
    parser.add_argument('-s', '--datasources', dest='datasource_dir', metavar='DATASOURCE_DIR', type=str, default='./datasources', nargs='?', help='directory in which to place *.csv files for limn')
    parser.add_argument('-f', '--datafiles', dest='datafile_dir', metavar='DATAFILE_DIR', type=str, default='./datafiles', nargs='?', help='direcotyr in which to place the *.yaml files for limn')
    parser.add_argument('-o', '--outfile', dest='outfile', metavar='BASE_FILENAME', type=str, default='geo_editors', help='base file name for csv and yaml files.  for example: DATASOURCE_DIR/BAS_FILENAME_en.yaml')

    args = parser.parse_args()
    log.info(json.dumps(vars(args), indent=2))
    return args

if __name__ == '__main__':
    log.info('cwd: %s' % (os.getcwd()))
    args = parse_args()
    json_all = load_json_files(args.geo_files)
    write_datasources(json_all, args)
