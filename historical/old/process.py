import argparse
import logging
import pprint
import datetime
import csv
from operator import itemgetter
import sys

from wikistats import process_metrics, core, ez, writers

"""
logging set up
"""
root_logger = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s]\t[%(threadName)s]\t[%(funcName)s:%(lineno)d]\t%(message)s')
ch.setFormatter(formatter)
root_logger.addHandler(ch)
root_logger.setLevel(logging.DEBUG)

LEVELS = {'DEBUG': logging.DEBUG,
          'INFO': logging.INFO,
          'WARNING': logging.WARNING,
          'ERROR': logging.ERROR,
          'CRITICAL': logging.CRITICAL}
    

class LoadTSVAction(argparse.Action):
    """
    This action is fired upon parsing the --projfiles option which should be a list of 
    tsv file names.  Each named file should have the wp project codes as the first column
    The codes will be used to query the databse with the name <ID>wiki.

    (Sorry about the nasty python functional syntax. will redo with moka shortly...)
    """
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, list(set(
                    map(
                        itemgetter(0),
                        map(
                            str.split,
                            filter(
                                lambda line: line[0] != '#',
                                reduce(
                                    list.__add__, 
                                    map(
                                        file.readlines,
                                        map(
                                            open,
                                            values)), [])))))))


def parse_args():
    parser = argparse.ArgumentParser('wikistats')
    parser.add_argument('--projects',
                        nargs='*',
                        help='a space delimited list of project identifiers, like en, de, it, zh or pt')
    parser.add_argument('--projfile',
                        dest='projects',
                        nargs='*',
                        action=LoadTSVAction,
                        help='a TSV file with project ids to process in the first column')
    parser.add_argument('--outfile',
                        default='wikistats_results.csv',
                        help='an outfile path to which to write the output csv')
    parser.add_argument('-l', '--loglevel',
                        default='DEBUG',
                        choices=LEVELS.keys(),
                        help='logging level')
    args = parser.parse_args()

    logging.getLogger().setLevel(LEVELS[args.loglevel])

    if not hasattr(args, 'projects') or not args.projects:
        parser.error('must specify the projects to analyze using the --projects option or the --projfile option')
    logging.info('\n' + pprint.pformat(vars(args)))
    return vars(args)


def main():
    opts = parse_args()
    timeseries_metrics = [
        core.RecurringMetric(ez.NewEditors),
        core.RecurringMetric(ez.ActiveEditors, min_edits=1),
        core.RecurringMetric(ez.ActiveEditors, min_edits=5),
        core.RecurringMetric(ez.ActiveEditors, min_edits=100),
        core.RecurringMetric(ez.NewArticles),
        # core.RecurringMetric(ez.NewArticles, rstart=datetime.datetime(2011,8,1)),
        ]
    wiki_metrics = [] + timeseries_metrics
    summary_metrics.extend([tm.limn_writer() for tm in timeseries_metrics])

    process_metrics(wiki_metrics, summary_metrics, opts)

if __name__ == '__main__':
    main()
