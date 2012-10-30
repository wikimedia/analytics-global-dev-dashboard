import pandas as pd
import csv
import limnpy

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



rdr = csv.reader(open(stats_fn))
lines = [line for line in rdr]
df = pd.DataFrame(lines,columns=fieldnames)

for val_key in val_keys:
    pt = df.pivot(index='date', columns='project', values=val_key)
    pt = pt.fillna(0)
    idx = pt.index
    pt_rows = [[idx[i]] + list(pt.irow(i)) for i in range(len(pt))]
    limn_id = 'overall_%s' % val_key
    limn_name = limn_id.replace('_', ' ').title()
    limnpy.write(limn_id, limn_name, ['date'] + list(pt.columns), pt_rows)
