import argparse
import logging as log
import json
from datetime import datetime
from collections import defaultdict, OrderedDict
from functools import partial
from nesting import Nest
from operator import itemgetter
import yaml, csv
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

def flatten(nest, path=[], keys=[]):
    #log.debug('entering with type(nest):%s,\tpath: %s' % (type(nest), path))
    # try to use as dict
    try:
        #log.debug('trying to use as dict')
        for k, v in nest.items():
            #log.debug('calling flatten(%s, %s)' % (k, v))
            for row in flatten(v, path + [k], keys):
                yield row
    except AttributeError:
        #log.debug('nest has not attribute \'items()\'')
        # try to use as list
        try:
            #log.debug('trying to use as list')
            for elem in nest:
                for row in flatten(elem, path, keys):
                    yield row
        except TypeError:
            #log.debug('nest object of type %s is not iterable' % (type(nest)))
            #log.debug('reached leaf of type: %s' % (type(nest)))
            # must be a leaf, finally yield
            #log.debug('yielding %s' % (path + [nest]))
            yield dict(zip(keys, path + [nest]))


def load_json_files(files):
    json_all = defaultdict(dict)
    projects = []
    for f in files:
        json_f = json.load(open(f, 'r'))
        projects.append(json_f['project'])
        json_all[get_date(f)][json_f['project']] = json_f['countries']
    # log.debug('f: %s' % (json.dumps(json_all, indent=2)))
    # expand tree structure of dictionaries into list of dicts with named fields
    rows = list(flatten(json_all, [], ['date', 'project', 'country', 'cohort', 'count']))
    for row in rows:
        row['country-cohort'] = row['country'] + '-' + row['cohort']
    by_date = Nest().key(itemgetter('date')).map(rows)
    # everything is by date, so everyone wants things sorted
    by_date = OrderedDict(sorted(by_date.items()))
    #log.debug('rows: %s', rows)
    log.debug('found projects: %s', projects)
    return by_date, projects


def write_yaml(_id, name, fields, csv_name, rows, args):

    meta = {}
    meta['id'] = _id
    meta['name'] = name
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

    yaml_name = args.basename + '_' + proj + '.yaml'
    yaml_path = os.path.join(args.datasource_dir, yaml_name)
    fyaml = open(yaml_path, 'w')
    fyaml.write(yaml.safe_dump(meta, default_flow_style=False))
    fyaml.close()
    return yaml_path


def write_project_datasource(proj, rows, args):
    log.debug('proj: %s,\tlen(rows): %s' % (proj, len(rows)))
    csv_name = args.basename + '_' + proj + '.csv'
    csv_path = os.path.join(args.datafile_dir, csv_name)
    csv_file = open(csv_path, 'w')

    # remove rows that don't interest us and then grab the row id (country-cohort) and count
    csv_rows = []
    for date, row_batch in rows.items():
        filtered_batch = filter(lambda row : row['project'] == proj, row_batch)
        csv_row = {'date' : date}
        for row in filtered_batch:
            csv_row[row['country-cohort']] = row['count']
        csv_rows.append(csv_row)

    # normalize fields
    all_fields = sorted(reduce(set.__ior__, map(set,map(dict.keys, csv_rows)), set()))

    writer = csv.DictWriter(csv_file, all_fields, restval='', extrasaction='ignore')
    writer.writeheader()
    for csv_row in csv_rows:
        writer.writerow(csv_row)
    csv_file.close() 

    #yaml_name = write_yaml('%s Editors' % proj.Upper(), rows, all_fields, csv_name, args)
    
    #return (csv_name, yaml_name)


def write_summary_graphs(json_all, args):
    for proj, rows in json_all.items():
        graph = {}
        graph['options'] = {
            "strokeWidth": 4,
            "pointSize": 4,
            "stackedGraph": true,
            "digitsAfterDecimal": 0,
            "drawPoints": true,
            "axisLabelFontSize": 12,
            "xlabel": "Date",
            "ylabel": "# Active Editors (>5 Edits)"
            }
        graph["name"] = "Arabic WP Active Editors by Country (stacked graph)",
        graph["notes"] = ""
        graph["callout"] = {
            "enabled": true,
            "metric_idx": 0,
            "label": ""
            }
        graph["slug"] = "ar_wp"
        graph["width"] = "auto"
        graph["parents"] = ["root"]
        graph["result"] = "ok"
        graph["id"] = "ar_wp"
        graph["chartType"] = "dygraphs"
        graph["height"] = 320
        metrics = []
        for i,  in enumerate(rows):
            if i >= k:
                break
            metric = {}
            metric["index"] = 1,
            metric["scale"] = 1,
            metric["timespan"] = {
            "start": null,
            "step": null,
            "end": null
            },
            metric["color"] = "#d53e4f",
            metric["format_axis"] = null,
            metric["label"] = "Algeria",
            metric["disabled"] = false,
            metric["visible"] = true,
            metric["format_value"] = null,
            metric["transforms"] = [],
            metric["source_id"] = "active_editors_ar",
            metric["chartType"] = null,
            metric["type"] = "int",
            metric["source_col"] = 5
            metrics.append(metric)
        data = {}
        data["metrics"] = metrics
        graph["data"] = data




def parse_args():

    parser = argparse.ArgumentParser(description='Format a collection of json files output by editor-geocoding and creates a single csv in digraph format.')
    parser.add_argument(
        'geo_files', 
        metavar='GEOCODING_FILE.json', 
        nargs='+',
        help='any number of appropriately named json files')
    parser.add_argument(
        '-s','--datasource_dir',
        default='./datasources',
        nargs='?',
        help='directory in which to place *.csv files for limn')
    parser.add_argument(
        '-f', '--datafile_dir',
        default='./datafiles',
        nargs='?', 
        help='directory in which to place the *.yaml files for limn')
    parser.add_argument(
        '-g', '--graphs_dir',
        default='./graphs', 
        nargs='?',
        help='directory in which to place the *.json which represent graph metadata')
    parser.add_argument(
        '-b', '--basename',
        default='geo_editors',
        help='base file name for csv and yaml files.  for example: DATASOURCE_DIR/BAS_FILENAME_en.yaml')
    parser.add_argument(
        '-k', 
        type=int, 
        default=10, 
        help='the number of countries to include in the selected project datasource')

    args = parser.parse_args()

    for name in [args.datafile_dir, args.datasource_dir, args.graphs_dir]:
        if not os.path.exists(name):
            os.makedirs(name)

    log.info(json.dumps(vars(args), indent=2))
    return args

if __name__ == '__main__':
    args = parse_args()
    rows, projects = load_json_files(args.geo_files)
    for project in projects:
        write_project_datasource(project, rows, args)
    #     write_project_datasource(project, rows args)
    # write_overall_datasource(projects, rows, args)
    # write_catalyst_datasource(projects, rows, args)

