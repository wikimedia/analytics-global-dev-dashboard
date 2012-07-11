import mwclient
from collections import defaultdict, OrderedDict, Counter
from operator import itemgetter, attrgetter
from threading import Thread
import argparse
import os
import logging as log
import re

# setup logging
log.basicConfig(filename=None, format='[%(levelname)s]\t[%(threadName)s]\t[%(funcName)s:%(lineno)d]\t%(message)s', level=log.DEBUG)

active_user_threshold = 5 # edits per month
#max_users = 40

def pad(i, n):
    zeros = ''.join(['0' for j in range(n - len(str(i)))])
    return zeros + str(i)

def year_mo(tstruct):
    return str(tstruct.tm_year) + '/' + pad(tstruct.tm_mon, 2) + '/01'

def get_activity_by_month(time_series):
    month_tallies = defaultdict(int)
    for time_stamp in time_series:
        month_tallies[year_mo(time_stamp)] += 1
    return month_tallies

def get_binned_active_user_count(active_user_count, bins=[1,3,5,20,50,100]):
    binned_counts = defaultdict(lambda: dict(zip(bins, [0]*len(bins)))) # make sure every month has each bin because they will be tsv columns
    for month, dist in active_user_count.items():
        for num_edits, count in dist.items():
            for threshold in bins:
                if count >= threshold:
                    binned_counts[month][threshold] += 1
    log.debug('binned_counts: %s' % (binned_counts))
    return binned_counts

def write_counts(binned_active_user_count, output_dir, proj, i=None):
    cache_name = 'active_editors.%s.%d.csv' % (proj, i)
    cache_path = os.path.join(output_dir, cache_name)
    binned_active_user_count_sorted = OrderedDict(sorted(binned_active_user_count.items(), key=itemgetter(0)))
    with open(cache_path, 'w') as fout:
        for month, countdict in binned_active_user_count_sorted.items():
            fout.write('%s,%s\n' % (month, ','.join(map(str, countdict.values()))))
    log.debug('wrote cache: %s' % (cache_path))
    
    # remove previous cache(s) from same project
    cache_pat = 'active_editors\.%s\.([0-9]+)\.csv' % (proj)
    for fname in os.listdir(output_dir):
        match = re.match(cache_pat, fname)
        if match and match.groups()[0] and int(match.groups()[0]) != i:
            #log.debug('match.groups()[0]: %d, i:%d int(groups()[0]) != i: %s' % (int(match.groups()[0]), i, int(match.groups()[0]) != i))
            cache_path = os.path.join(output_dir, fname)
            log.debug('removing file: %s' % (cache_path))
            os.remove(cache_path)


def count_active_users(proj, options):
    try:
        site = mwclient.Site('%s.wikipedia.org' % (proj))
    except:
        log.error('could not create site for proj: %s' % (proj))
    if site:
        users = site.allusers(site, prop='groups|editcount')
        active_user_count = defaultdict(lambda: defaultdict(int))
        for i, user in enumerate(users):
            if not user['editcount']:
                #log.debug('skipping user with editcount: %d' % (user['editcount']))
                continue
            if 'bot' in user['groups']:
                #log.debug('skipping user with groups: %s' % (user['groups']))
                continue
            contribs = site.usercontributions(user['name'], namespace=0)
            times = map(itemgetter(u'timestamp'), contribs)
            if (not times):
                continue # sometimes they have edits in other namespaces but not this one
            activity_by_month = get_activity_by_month(times)
            for month, activity in activity_by_month.items():
                active_user_count[month][activity] += 1
            if i > options.max_users:
                break
            if i % 100 == 0:
                log.info('processed: %d users' % (i))
                binned_active_user_count = get_binned_active_user_count(active_user_count)
                write_counts(binned_active_user_count, options.output_dir, proj, i)
        binned_active_user_count = get_binned_active_user_count(active_user_count)
        write_counts(binned_active_user_count, i, options.output_dir, proj)


def parse_args():
    parser = argparse.ArgumentParser(description='Uses MediaWiki API to grab low activity langauge counts')
    parser.add_argument('lang_file', type=str, help='a tsv file of the form: <proj_id> <proj_full_name>')
    parser.add_argument('-m', '--max_users', type=int, default=(), help='maximum number of users per proj before exiting (useful for development)')
    parser.add_argument('-o', '--output', dest='output_dir', type=str, default='./output', help='output directory for (incremental) csv files')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    lang_file = open(args.lang_file)
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    threads = []
    for line in lang_file:
        if line[0] == '#':
            continue
        id = line.split()[0]
        t = Thread(target=count_active_users, args=(id,args))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

