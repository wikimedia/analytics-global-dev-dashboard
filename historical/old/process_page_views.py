import pandas as pd
import csv
import limnpy
import datetime

basedir = '/home/erosen/src/dashboard/historical/data'


pv_fn='/a/wikistats_git/dumps/csv/csv_wp/PageViewsPerMonthAll.csv'

fieldnames = ['project',
              'date',
              'page_views']

val_keys = ['page_views']

df_long = pd.read_table(pv_fn, sep=',', header=None, parse_dates=['date'], names=fieldnames)

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
