#!/usr/bin/python
import logging
import limnpy
import datetime
import gcat
import pandas as pd
from iso3166 import approx_countries as countries
import json

"""
    Map of grant spend (Exclude the FDC - WMF line)
    Grant spend over time (stacked bar chart by grant type. We don't have months, so do it by Half year (column H "Timing"))
    Grant spend by GS & GN over time (stacked bar - GS and GN - by half year) 
    # of grants over time (stacked bar chart by grant type. We don't have months, so do it by Half year (column H "Timing"))
    # grants by GS and GN over time 
    # editors in GS/GN (sams as you have)
    GS Editors as a % of total editors: just track the % over time
"""

root_logger = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s]\t[%(threadName)s]\t[%(funcName)s:%(lineno)d]\t%(message)s')
ch.setFormatter(formatter)
root_logger.addHandler(ch)
root_logger.setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)

def plot_spending_by_country(df, country_df, basedir):
   #{
        #"editors100": 221,
        #"editors5": 2088,
        #"editors": 6666,
        #"id": "CAN",
        #"name": "Canada"
    #},
    merged = pd.merge(df, country_df, left_on='Location', right_on='MaxMind Country')
    plt_df = merged.groupby(['Location', 'ISO-3166 Alpha-3'])['Amount Funded'].sum().reset_index()
    ds = []
    for idx, row in plt_df.iterrows():
        d = {}
        d['id'] = row['ISO-3166 Alpha-3']
        d['name'] = row['Location']
        d['amount_funded'] = row['Amount Funded']
        ds.append(d)
    print json.dumps(ds, indent=4)

def plot_spending_over_time(df, basedir):
    #plot_df = df.groupby('Timing').sum()[['Amount Funded']]
    plot_df = pd.DataFrame.pivot_table(df,
            rows='Timing',
            cols='Grant Type', 
            values='Amount Funded', 
            aggfunc=sum)
    print plot_df

    ds = limnpy.DataSource(limn_id='grants_spending_over_time',
            limn_name='Spending Over Time',
            data=plot_df)
    ds.write(basedir)
    g = ds.write_graph(basedir=basedir)
    #metric_group_node = g.graph['root']['children'][limnpy.Graph.METRIC_CHILD_ID]
    #metric_group_node['nodeType'] = 'bar-group'
    #for child_node in metric_group_node['children']:
        #child_node['nodeType'] = 'bar'
    g.write(basedir)

def plot_spending_by_global_south(df, country_df, basedir):
    merged = pd.merge(df, country_df, left_on='Location', right_on='MaxMind Country')
    plot_df = pd.DataFrame.pivot_table(merged,
            rows='Timing',
            cols='Global South/North',
            values='Amount Funded',
            aggfunc=sum)

    ds = limnpy.DataSource(limn_id='grants_spending_by_global_south',
            limn_name='Spending by Global South',
            data=plot_df)
    ds.write(basedir)
    ds.write_graph(basedir=basedir)
    print merged

def plot_grants_over_time(df, basedir):
    pass

def plot_grants_by_global_south(df, basedir):
    pass

def clean_location(c_str):
    if c_str == 'N/A':
        return None
    return countries[c_str].name

def get_grant_data():
    file_title = '(L&E) Grants data FY2012-13_2'
    ex = gcat.get_file(file_title, fmt='pandas_excel', usecache=True)
    df = ex.parse('Grants Data', skip_footer=23).ix[:,:8]
    df['Timing'].replace({
        'Q1-Q2 2012-13' : datetime.date(2012,12,1),
        'Q3-Q4 2012-13' : datetime.date(2013,6,1),
        'Q1-Q2 2013-14' : datetime.date(2013,12,1),
        'Q3-Q4 2013-14' : datetime.date(2014,6,1),
        }, inplace=True)
    df['Global South/North'].replace({'Undefined':'N/A'}, inplace=True)
    df['Timing'] = pd.to_datetime(df['Timing'])
    df['Location'] = df['Location'].apply(clean_location)
    return df

def get_country_data():
    country_df = gcat.get_file('Global South and Region Classifications', 
            sheet='data',
            fmt='pandas',
            usecache=True)
    country_df = country_df[country_df['ISO-3166 Alpha-2'].notnull()]
    country_df['MaxMind Country'] = country_df['MaxMind Country'].apply(clean_location)
    return country_df

def main():
    basedir = 'data'
    df = get_grant_data()
    print df
    country_df = get_country_data()
    print country_df
    plot_spending_by_country(df, country_df, basedir)
    plot_spending_over_time(df, basedir)
    plot_spending_by_global_south(df, country_df, basedir)
    plot_grants_over_time(df, basedir)
    plot_grants_by_global_south(df, basedir)

if __name__ == '__main__':
    main()
