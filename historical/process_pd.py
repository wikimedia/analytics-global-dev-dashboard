import pandas as pd
import csv
import limnpy
import datetime

stats_fn='/a/wikistats/csv/csv_wp/StatisticsMonthly.csv'

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

df = pd.read_csv(stats_fn, names=fieldnames, parse_dates=['date'])
df['date'] = df['date'].apply(datetime.datetime.date)

indic_lang_df = pd.read_csv('../data/indic_lang_ids.tsv', names=['id','name'], sep='\t', comment='#')
indic_langs = indic_lang_df['id'].dropna().unique()

for val_key in val_keys:
    pt = df.pivot(index='date', columns='project', values=val_key)
    pt = pt.fillna(0)
    limn_id = 'overall_%s' % val_key
    limnpy.write(limn_id, limn_id.replace('_', ' ').title(), list(pt.reset_index().columns), pt.itertuples())

    indic_pt = pt[indic_langs].fillna(0).sum(axis=1)
    indic_limn_id = 'indic_language_%s' % val_key
    limnpy.write(indic_limn_id, indic_limn_id.replace('_', ' ').title(), list(indic_pt.reset_index().columns), indic_pt.iteritems())
