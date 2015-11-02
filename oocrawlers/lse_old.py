import requests
from lxml import html
import re
import dataset
from dateutil import parser
import os
import random

BASEDIR = '/data/lse/news_mining/'

db = dataset.connect('sqlite:///lse_mining.sqlite')
#db = dataset.connect('postgresql://openoil:EJLENtQZ2Lp766wB9tD8@localhost/openoil')

def newsitems_from_page_old(pagetext, companycode):
    # return list of tuples (id, date, title)
    lx = html.fromstring(pagetext)
    results = []
    for row in lx.cssselect('li.newsContainer'):
        as_str = str(html.tostring(row))
        newsid = re.search('market-news-detail/(\d+).html', as_str).group(1)
        title = row.cssselect('a')[0].text.strip()
        postdate = parser.parse(row.cssselect('span.hour')[0].text, fuzzy = True)
        results.append({
            'newsid': newsid,
            'title': bytes(title, 'ascii', errors='ignore'),
            'postdate': postdate,
            'company': companycode})
    return results

def newsitems_from_page(pagetext, companycode):
    # return list of tuples (id, date, title)
    lx = html.fromstring(pagetext)
    results = []
    for row in lx.cssselect('.table_datinews tr'):
        as_str = str(html.tostring(row))
        newsid = re.search('market-news-detail/(\d+).html', as_str).group(1)
        title = row.cssselect('a')[0].text.strip()
        postdate = parser.parse(row.cssselect('.datetime')[0].text, fuzzy = True)
        results.append({
            'newsid': newsid,
            'title': title,
            'postdate': postdate,
            'company': companycode})
    return results


def listings_for_company(company):
    table = db['lse_news_items']
    url = 'http://www.londonstockexchange.com/exchange/prices-and-markets/stocks/exchange-insight/news-analysis.html'
    itemcount = 0
    for pagenum in range(1,999):
        params = {
            'fourWayKey': company['company_id'],
            'page': pagenum}
        pagetext = requests.get(url, params=params).text
        newitems = newsitems_from_page(pagetext, company['company_code'])
        if not newitems:
            break
        for item in newitems:
            table.upsert(item, ['newsid'])
            itemcount += 1
    return itemcount


def get_company_basic_details(sector='0530'):
    baseurl = 'http://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/constituents-indices.html'
    for pagenum in range(1,999):
        print('page %s' % pagenum)
        params = {
            'industrySector': sector,
            'index': '',
            'page': pagenum}
        pt = requests.get(baseurl,
                          params=params,
                          # something messes up if we allow compression
                          headers = {'Accept-Encoding': ''},
                      ).text
        newcomps = comps_from_page(pt, sector)
        print('page %s: %s new companies' % (pagenum, newcomps))
        if not newcomps:
            break

def comps_from_page(pt, industry_code):
    table = db['company_details']
    h = html.fromstring(pt)
    newcompanies = 0
    for row in h.cssselect('table.table_dati>tbody>tr'):
        data = {}
        data['industry_code'] = industry_code
        #data['company_id'] = row.cssselect('a')[0].attrib['href'].split('=')[-1]
        data['company_id'] = re.search('([^/]*).html', row.cssselect('a')[0].attrib['href']).group(1)
        data['company_code'] = row.find('td').text.strip()
        data['company_name'] = row.cssselect('a')[0].text
        if list(table.find(company_id = data['company_id'])):
            continue
        newcompanies += 1
        table.upsert(data, ['company_id'])
    return newcompanies


def scrape_all_oil():
    get_company_basic_details('0530')
    companies = [x for x in db['company_details'].find()]
    import time
    time.sleep(0.1)
    for company in companies:
        #if list(db['lse_news_items'].find(company=company['company_code'])):
        #    print('skipping already-dled company %s' % company['company_code'])
        #    continue
        cnt = listings_for_company(company)
        print('%s: got %s filings' % (company['company_code'], cnt))
    dl_all_filings()


def scrape_all_mining():
    get_company_basic_details('1770')
    companies = [x for x in db['company_details'].find()]
    import time
    time.sleep(0.1)
    for company in companies:
        #if list(db['lse_news_items'].find(company=company['company_code'])):
        #    print('skipping already-dled company %s' % company['company_code'])
        #    continue
        cnt = listings_for_company(company)
        print('%s: got %s filings' % (company['company_code'], cnt))
    dl_all_filings()

    
def dl_all_filings():
    newsitems = list(db['lse_news_items'].all())
    random.shuffle(newsitems)
    for item in newsitems:
        url = 'http://www.londonstockexchange.com/exchange/news/market-news/market-news-detail/%s.html' % item['newsid']
        dir = BASEDIR + item['company']
        if not os.path.exists(dir):
            os.makedirs(dir)
        fn = '%s/%s.html' % (dir, item['newsid'])
        if os.path.exists(fn):
            print('skip existing file %s' % fn)
            continue
        pt = requests.get(url).text
        open(fn, 'w').write(pt)
        
def test():
    pt = requests.get('http://www.londonstockexchange.com/exchange/prices-and-markets/stocks/exchange-insight/company-news.html?fourWayKey=IE00B034YN94IEGBXAIM').text
    return newsitems_from_page(pt)

def test_key():
    fourwaykey = 'IE00B034YN94IEGBXAIM'
    return listings_for_company(fourwaykey)
