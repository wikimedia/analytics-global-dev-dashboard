import pandas as pd
import csv
import limnpy
import datetime

basedir = '/home/erosen/src/dashboard/historical/data'

stats_fn='/a/wikistats/csv/csv_wp/StatisticsMonthly.csv'
stats_fn='/a/wikistats_git/dumps/csv/csv_wp/StatisticsMonthly.csv'

fieldnames = ['project',
              'date',
              'total_editors',
              'new_editors',
              'active_editors', 
              'very_active_editors',
              'num_articles',
              'num_articles_200',
              'new_articles_per_day', 
              'mean_edits',
              'mean_bytes', 
              'larger_than_0.5kb', 
              'larger_than_2kb',
              'db_num_edits', 
              'db_size', 
              'db_num_words', 
              'num_links_internal', 
              'num_links_interwiki',
              'num_links_image', 
              'num_links_external',
              'num_links_redirects'] + range(8)

val_keys = ['total_editors',
              'new_editors',
              'active_editors', 
              'very_active_editors',
              'new_articles_per_day', 
              'mean_edits',
              'num_links_internal', 
              'num_links_interwiki',
              'num_links_image', 
              'num_links_external',
              'num_links_redirects']

df = pd.read_table(stats_fn, sep=',', header=None, parse_dates=['date'], names=fieldnames)
df.set_index('date')

indic_lang_df = pd.read_table('../data/indic_lang_ids.tsv', sep='\t', comment='#', names=['id','name'])
indic_langs = indic_lang_df['id'].dropna().unique()

for val_key in val_keys:
    pt = df.pivot(index='date', columns='project', values=val_key)
    pt = pt.fillna(0)
    limn_id = 'overall_%s' % val_key
    limn_title = limn_id.replace('_', ' ').title()
    ds = limnpy.DataSource(limn_id, limn_title, pt)
    ds.write(basedir)

    indic_pt = pd.DataFrame(pt[indic_langs].fillna(0).sum(axis=1), columns=[val_key])
    print indic_pt.index
    indic_limn_id = 'indic_language_%s' % val_key
    indic_limn_title = indic_limn_id.replace('_', ' ').title()
    indic_ds = limnpy.DataSource(indic_limn_id, indic_limn_title, indic_pt)
    indic_ds.write(basedir)
