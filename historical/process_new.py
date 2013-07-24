import pandas as pd
import csv
import limnpy
import datetime

basedir = '/home/erosen/src/dashboard/historical/data'

stats_fn='/a/wikistats_git/dumps/csv/csv_wp/StatisticsUserActivitySpread.csv'

fieldnames = ['
        1  3  5  10  25  100  250  1000  2500  10000  25000           #Articles Users
              5  10      100       1000        10000         100000   #Articles Bots 
        1  3  5  10  25  100  250  1000  2500  10000  25000  100000   #Talk Users
              5  10      100       1000        10000         100000   #Talk Bots
        1  3  5  10  25  100  250  1000  2500  10000  25000           #Other Users 
              5  10      100       1000        10000         100000'] #Other Bots

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
              'num_articles',
              'new_articles_per_day', 
              'mean_edits',
              'db_size',
              'num_links_internal', 
              'num_links_interwiki',
              'num_links_image', 
              'num_links_external',
              'num_links_redirects']

df_long = pd.read_table(stats_fn, sep=',', header=None, parse_dates=['date'], names=fieldnames)

print df_long

indic_lang_df = pd.read_table('../data/indic_lang_ids.tsv', sep='\t', comment='#', names=['id','name'])
indic_langs = indic_lang_df['id'].dropna().unique()

for val_key in val_keys:
    print 'processing val_key: %s' % val_key
    df = df_long.pivot(index='date', columns='project', values=val_key)
    # print pt
    limn_id = 'overall_%s' % val_key
    limn_title = limn_id.replace('_', ' ').title()
    ds = limnpy.DataSource(limn_id, limn_title,df)
    ds.write(basedir)



    indic_df = df[indic_langs]
    indic_df['Total'] = indic_df.sum(axis=1)
    indic_limn_id = 'indic_language_%s' % val_key
    indic_limn_title = indic_limn_id.replace('_', ' ').title()
    indic_ds = limnpy.DataSource(indic_limn_id, indic_limn_title, indic_df)
    # print indic_pt
    indic_ds.write(basedir)
    indic_ds.write_graph(indic_langs, basedir=basedir)
