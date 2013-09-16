#!/usr/bin/python
import logging
import limnpy
import datetime
import gcat
import pandas as pd
from iso3166 import approx_countries as countries
import os
import json
import numpy as np

"""
X    Map of grant spend (Exclude the FDC - WMF line)
X    Grant spend over time (stacked bar chart by grant type. We don't have months, so do it by Half year (column H "Timing"))
X    Grant spend by GS & GN over time (stacked bar - GS and GN - by half year) 
X    # of grants over time (stacked bar chart by grant type. We don't have months, so do it by Half year (column H "Timing"))
X    # grants by GS and GN over time 
X    # editors in GS/GN (sams as you have)
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
    geo_df = merged.groupby(['Location', 'ISO-3166 Alpha-3'])['Amount Funded'].sum().reset_index()
    geo_df = geo_df.rename(columns={'Location' : 'name', 'ISO-3166 Alpha-3' : 'id', 'Amount Funded' : 'amount_funded'})

    geo_id = 'grants_spending_by_country'
    geo_name = "Grants Spending by Country",

    # write json format datafile
    geo_data = [r[1].to_dict() for r in geo_df.iterrows()]
    geo_df_file = open(os.path.join(basedir, 'datafiles', '%s.json' % geo_id), 'w')
    json.dump(geo_data, geo_df_file, indent=4)

    # write datasource
    ds = {
        "id": geo_id,
        "slug": geo_id,
        "format": "json",
        "type": "mobile_device_by_geo",
        "url": "/data/datafiles/gp/grants_spending_by_country.json",
        "name": geo_name, 
        "shortName": "",
        "desc": "",
        "notes": "",
        "columns": [
            {
                "id": "id",
                "label": "ID",
                "type": "string",
                #"index": 0
            },
            {
                "id": "name",
                "label": "Name",
                "type": "string",
                #"index": 1
            },
            {
                "id": "amount_funded",
                "label": "Amount Funded",
                "type": "int",
                #"index": 2
            },
        ]
    } 
    geo_ds_file = open(os.path.join(basedir, 'datasources', '%s.json' % geo_id), 'w')
    json.dump(ds, geo_ds_file, indent=4)

    graph ={
        "graph_version": "0.6.0",
        "id": geo_id,
        "slug": geo_id,
        "name": geo_name,
        "shortName": "",
        "desc": "",
        "notes": "",
        "root": {
            "nodeType": "canvas",
            "disabled": False,
            "children": [
                {
                    "nodeType": "geo-map",
                    "disabled": False,
                    "metric": {
                        "source_id": "map-world_countries",
                        "type": "int"
                    },
                    "options": {
                        "projection": "mercator",
                        "featuresColor": "#EEEEEE",
                        "backgroundColor": "white"
                    },
                    "children": [
                        {
                            "nodeType": "geo-feature",
                            "disabled": False,
                            "metric": {
                                "source_id": geo_id, 
                                "source_col": "amount_funded",
                                "type": "int"
                            },
                            "options": {
                                "label": "Amount Funded",
                                "scale": "log",
                                "valueFormat": ",.2s",
                                "fill": [
                                    "#D4E7ED",
                                    "#0A3A4B"
                                ]
                            },
                            "stroke": {
                                "width": 3,
                                "color": "#FFFFFF",
                                "opacity": [
                                    0,
                                    1
                                ]
                            }
                        },
                        {
                            "nodeType": "zoom-pan",
                            "disabled": False,
                            "options": {
                                "min": 1,
                                "max": 10
                            }
                        },
                        {
                            "nodeType": "infobox",
                            "disabled": False
                        }
                    ]
                }
            ],
            "width": "auto",
            "minWidth": 750,
            "height": 750,
            "minHeight": 500,
            "xPadding": 0,
            "yPadding": 0.1
        }
    }
    geo_graph_file = open(os.path.join(basedir, 'graphs', '%s.json' % geo_id), 'w')
    json.dump(graph, geo_graph_file, indent=4)
 

def plot_spending_over_time(df, basedir):
    #plot_df = df.groupby('Timing').sum()[['Amount Funded']]
    plot_df = pd.DataFrame.pivot_table(df,
            rows='Timing',
            cols='Grant Type', 
            values='Amount Funded', 
            aggfunc=sum)
    ds = limnpy.DataSource(limn_id='grants_spending_by_program',
            limn_name='Spending by Program',
            limn_group='gp',
            data=plot_df)
    ds.write(basedir)
    g = ds.get_graph()
    make_bar_graph(g)
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
            limn_group='gp',
            data=plot_df)
    ds.write(basedir)
    g = ds.get_graph()
    make_bar_graph(g)
    g.write(basedir)

def plot_grants_over_time(df, basedir):
    #plot_df = df.groupby('Timing').sum()[['Amount Funded']]
    plot_df = pd.DataFrame.pivot_table(df,
            rows='Timing',
            cols='Grant Type',
            values='Amount Funded',
            aggfunc=np.size)
    print plot_df

    ds = limnpy.DataSource(limn_id='grants_count_by_program',
            limn_name='Number of Grants by Program',
            limn_group='gp',
            data=plot_df)
    ds.write(basedir)
    g = ds.get_graph()
    make_bar_graph(g)
    g.write(basedir)

def plot_grants_by_global_south(df, country_df, basedir):
    merged = pd.merge(df, country_df, left_on='Location', right_on='MaxMind Country')
    plot_df = pd.DataFrame.pivot_table(merged,
            rows='Timing',
            cols='Global South/North',
            values='Amount Funded',
            aggfunc=len)

    ds = limnpy.DataSource(limn_id='grants_count_by_global_south',
            limn_name='Number of Grants by Global South',
            limn_group='gp',
            data=plot_df)
    ds.write(basedir)
    g = ds.get_graph()
    make_bar_graph(g)
    g.write(basedir)

def make_bar_graph(g, stacked=True):
    metric_group_node = g.graph['root']['children'][limnpy.Graph.METRIC_CHILD_ID]
    metric_group_node['nodeType'] = 'bar-group'
    metric_group_node['options']['stack'] = {'enabled' : True}
    for child_node in metric_group_node['children']:
        child_node['nodeType'] = 'bar'

def make_map_graph(g):
    graph_nodes = g.graph['root']['children']
    g.graph['root']['children'] = graph_nodes[limnpy.Graph.METRIC_CHILD_ID:]
    geo_node = g.graph['root']['children'][0]
    geo_node['nodeType'] = 'geo-map'
    for child_node in geo_node['children']:
        child_node['nodeType'] = 'geo-feature'

    zoom_pan_node = {
        "nodeType": "zoom-pan",
        "disabled": False,
        "options": {
            "min": 1,
            "max": 10
        }
    }
    infobox_node = {
        "nodeType": "infobox",
        "disabled": False
    } 
    geo_node['children'].append(zoom_pan_node)
    geo_node['children'].append(infobox_node)

def clean_location(c_str):
    if c_str == 'N/A':
        return None
    return countries[c_str].name

def get_grant_data():
    file_title = '(L&E) Grants data FY2012-13_2'
    ex = gcat.get_file(file_title, fmt='pandas_excel', usecache=False)
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
    country_df = get_country_data()
    plot_spending_by_country(df, country_df, basedir)
    plot_spending_over_time(df, basedir)
    plot_spending_by_global_south(df, country_df, basedir)
    plot_grants_over_time(df, basedir)
    plot_grants_by_global_south(df, country_df, basedir)

if __name__ == '__main__':
    main()
