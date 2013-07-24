#!/usr/bin/python
import json
import datetime
import multiprocessing
from collections import namedtuple
from dateutil import rrule
import pandas as pd
import requests
import logging
from sqlalchemy import create_engine, Column, Integer, Boolean, DateTime, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
import limnpy

logging.basicConfig()
logger = logging.getLogger(__name__)

engine_url = URL(drivername='mysql', host='s7-analytics-slave.eqiad.wmnet',
    database='metawiki',
    query={ 'read_default_file' : '/home/erosen/.my.cnf' }
)

Base = declarative_base()
Base.metadata.bind = create_engine(engine_url)
Session = sessionmaker(bind=Base.metadata.bind)

class Page(Base):
    __tablename__ = 'page'

    page_id = Column(Integer, primary_key=True)
    page_namespace = Column(Integer)
    page_title = Column(String(255))
    page_restrictions = None  # TODO: tinyblob NOT NULL,
    page_counter = None  # TODO: bigint(20) unsigned NOT NULL DEFAULT '0',
    page_is_redirect = Column(Boolean)
    page_is_new = Column(Boolean)
    page_random = None  # TODO: double unsigned NOT NULL,
    page_touched = Column(DateTime)
    page_latest = Column(Integer)
    page_len = Column(Integer)
    #page_content_model = Column(String(32))

Batch = namedtuple('Batch', ['name', 'title', 'prefix', 'start', 'end'])

def stats_url_for(page, year, month):
    return 'http://stats.grok.se/json/meta.m/{0}{1:02d}/{2}'.format(year, month, page.page_title)

def wiki_url_for(page):
    return 'http://meta.wikimedia.org/wiki/{0}'.format(page.page_title)

def get_pageviews((page, start, end)):
    pageviews = pd.Series()
    for d in rrule.rrule(rrule.MONTHLY, dtstart=start, until=end):
        r = requests.request('GET', stats_url_for(page, d.year, d.month))
        if r.status_code != 200:
            logger.warning('received status code %d for page: %s', r.status_code, r.url)
            continue
        try:
            parsed = json.loads(r.text)
        except ValueError:
            logger.exception('could not decode JSON:\n%s', r.text)
            continue
        month_views_dict = {}
        for dstr, v in parsed['daily_views'].iteritems():
            try:
                date_parsed = datetime.datetime.strptime(dstr, '%Y-%m-%d')
            except:
                logger.warning('could not parse date: %s', dstr)
                continue
            month_views_dict[date_parsed] = v
        pageviews = pageviews.append(pd.Series(month_views_dict))
    return page.page_title, pageviews

def get_batch_pageviews(batch, session):
    pages = session.query(Page).filter(Page.page_title.startswith(batch.prefix)).all()
    #pages = pages[:3]
    args = [(page, batch.start, batch.end) for page in pages]
    pool = multiprocessing.Pool(50)
    pageviews_iter = pool.map(get_pageviews, args)
    #pageviews_iter = map(get_pageviews, args)
    pageviews = pd.DataFrame.from_dict(dict(pageviews_iter))
    print pageviews
    print pageviews.index
    print pageviews.columns
    return pageviews

def main():
    page_batches = [
            Batch('round-1', 
                'FDC Round 1 Proposal Page Views',
                'FDC_portal/Proposals/2012-2013_round1',
                start=datetime.date(2012,6,1),
                end=datetime.date(2013,1,1)),
            Batch('round-2',
                'FDC Round 2 Proposal Page Views',
                'FDC_portal/Proposals/2012-2013_round2',
                start=datetime.date(2013,1,1),
                end=datetime.date(2013,7,1))]
    session = Session()
    for batch in page_batches:
        df = get_batch_pageviews(batch, session)
        df.to_csv('{0}-pageviews.csv'.format(batch.name))
        ds = limnpy.DataSource(limn_id='fdc-{0}'.format(batch.name),
                limn_name=batch.title,
                limn_group='gp',
                data=df)
        ds.write(basedir='data')
        ds.write_graph(basedir='data')

if __name__ == '__main__':
    main()
