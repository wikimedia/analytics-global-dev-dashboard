import logging
import dateutil.parser
import argparse
import pandas as pd
import os

logger = logging.getLogger(__name__)

FIELDS = ['date', 'lang', 'project', 'site', 'country', 'provider']
COUNT_FIELDS = ['count'] + FIELDS

def parse_args():
    parser = argparse.ArgumentParser('loads cached squid log count files and selects a specific date range')
    parser.add_argument('--cache_dir', default='zero_counts')
    parser.add_argument('--start',required=True)
    parser.add_argument('--end',required=True)
    parser.add_argument('-o','--outfile', default=None)
    
    opts = vars(parser.parse_args())
    opts['start'] = dateutil.parser.parse(opts['start'])
    opts['end'] = dateutil.parser.parse(opts['end'])
    if opts['outfile'] is None:
        opts['outfile'] = 'zero_report_%s_%s' % (opts['start'].strftime('%Y%m%d'), opts['end'].strftime('%Y%m%d'))
    return opts

def load(cache_dir):
    cache = pd.DataFrame(columns=COUNT_FIELDS)
    date_col_ind = COUNT_FIELDS.index('date')
    num_cached = len(os.listdir(cache_dir))
    logger.info('loading %d cached files from %s', num_cached, cache_dir)
    for i, count_file in enumerate(os.listdir(cache_dir)):
        # if i > 2:
        #     break
        try:
            df = pd.read_table(os.path.join(cache_dir, count_file), parse_dates=[date_col_ind], sep=',',  header=None)
            df.columns = COUNT_FIELDS
            logging.debug('loaded %d lines from %s (%d / %d)', len(df), count_file, i, num_cached)
            cache = cache.append(df)
        except:
            logger.exception('exception caught while loading cache file: %s', count_file)
    return cache

def main():
    opts = parse_args()
    cache = load(opts['cache_dir'])
    cache = cache[cache['date'] >= opts['start']]
    cache = cache[cache['date'] < opts['end']]
    cache.to_csv(opts['outfile'], index=False)

if __name__ == '__main__':
    main()
