import os
import tempfile
import ftplib
import logging
import itertools
from lxml import etree

from aleph.crawlers import Crawler, TagExists


log = logging.getLogger(__name__)

EDGNS = '{http://www.sec.gov/Archives/edgar}'
SICS = [
    1000, 1040, 1090, 1220, 1221, 1400 # mining
     ] + [1311, 1381, 1382, 1389
          ,2911, 2990,3532,3533,5171,5172,6792] # oil

class EdgarCrawler(Crawler):

    LABEL = "SEC EDGAR"
    SITE = "http://sec.gov"
    MONTHS = 4000
    OFFSET = 0

    def monthly_indexes(self):
        ftp = ftplib.FTP('ftp.sec.gov')
        ftp.login('anonymous', '@anonymous')
        ftp.cwd('edgar/monthly')
        for file_name in ftp.nlst()[self.OFFSET:self.MONTHS]:
            with tempfile.NamedTemporaryFile(suffix='.xml') as fh:
                ftp.retrbinary("RETR " + file_name, fh.write)
                fh.seek(0)
                yield fh.name
        ftp.quit()

    def parse_feed(self, file_name):
        doc = etree.parse(file_name)
        for item in doc.findall('.//item'):
            data = {}
            for c in item.iterchildren():
                if EDGNS in c.tag:
                    continue
                if c.tag == 'enclosure':
                    data[c.tag] = c.get('url')
                else:
                    data[c.tag] = c.text

            for fc in item.findall(EDGNS + 'xbrlFiling/*'):
                tag = fc.tag.replace(EDGNS, '')
                if tag == 'xbrlFiles':
                    continue

                if fc.text:
                    data[tag] = fc.text

            log.debug('Filing title: %s', data.get('title'))

            if data.get('assignedSic') is None or \
                    int(data['assignedSic']) not in SICS:
                continue

            for fc in item.findall(EDGNS + 'xbrlFiling//' + EDGNS + 'xbrlFile'):
                file_data = data.copy()
                for k, v in fc.attrib.items():
                    file_data[k.replace(EDGNS, 'file_')] = v

                url = file_data.get('file_url', '')
                _, ext = os.path.splitext(url.lower())
                if ext in ['.htm', '.html', '.txt', '.pdf']:
                    yield url, file_data

    def crawl(self):
        for file_path in self.monthly_indexes():
            logging.info('working on %s' % file_path)
            try:
                for url, file_data in self.parse_feed(file_path):
                    try:
                        self.check_tag(url=url)
                        title = u'%s - %s' % (file_data.get('file_description'),
                                              file_data.get('title'))
                        self.emit_url(url, title=title,
                                      summary=file_data.get('file_description'),
                                      meta=file_data)
                        log.info('emitted %s' % url)
                    except TagExists:
                        log.info('skipping existing tag %s' % url)
                        pass
            except Exception, e:
                log.error('Monthly set: %r', file_path)
                log.exception(e)
