import pprint

class TagExists(Exception): pass

class TestCrawler(object):
    MAX_RESULTS = 10

    def __init__(self, *args, **kwargs):
        self.results = []

    def emit_url(self, url, package_id=None, **kwargs):
        pprint.pprint([url, kwargs])
        self.results.append([url, package_id, kwargs])
        if len(self.results) >= self.MAX_RESULTS:
            raise RuntimeError('finished downloading some items')

    def check_tag(self, *args, **kwargs):
        return

Crawler = TestCrawler

    

class ExampleCrawler(Crawler):

    LABEL = "Name of Site"
    SITE = "http://www.sgx.com/"


    def list_companies(self):
        ## Step 1: find companies from http://www.sgx.com/wps/portal/sgxweb/home/company_disclosure/stockfacts
        ## return an iterable of the basic metadata for each company
        return [
            ['Alliance Mineral Assets Limited',{'code': '40F' , 'industry': 'Metals and Mining'}],
            #...
            ]

    def get_detailed_metadata(self, url):
        ## from an url like http://infopub.sgx.com/Apps?A=COW_CorpAnnouncement_Content&B=AnnouncementLast12MonthsSecurity&F=D2M3PWUVM5TG6F26&H=96bbc8719840ba378779b2987f27f37452158ff15d8b7cb79a7b34927db58945

        ## give us the url of each of the attachments, and the metadata collected from the announcement detail page
        return [        ['http://infopub.sgx.com/Apps?A=COW_CorpAnnouncement_Content&B=AnnouncementLast12MonthsSecurity&F=D2M3PWUVM5TG6F26&H=96bbc8719840ba378779b2987f27f37452158ff15d8b7cb79a7b34927db58945&fileId=Announcement.pdf',],
            {'Issuer/Manager': 'ALLIANCE MINERAL ASSETS LIMITED',
             'Securities': 'ALLIANCE MINERAL ASSETSLIMITED - AU0000XINEV7 - 40F'}
            ]


    def announcements_for_company(self, company_name):
        ## Get all the announcements for the company from 
        return [ #list of [url, anouncement_metadata] 
['http://infopub.sgx.com/Apps?A=COW_CorpAnnouncement_Content&B=AnnouncementLast12MonthsSecurity&F=D2M3PWUVM5TG6F26&H=96bbc8719840ba378779b2987f27f37452158ff15d8b7cb79a7b34927db58945', {'Announcement Title': 'Financial Statements and Related Announcement::Full Yearly Results'}],
                #...
        ]
    
    def crawl(self):
        for company_name, basic_company_data in self.list_companies():
            for (announcement_url, announcement_metadata) in self.announcements_for_company(company_name):
                attachment_urls, detailed_metadata = self.get_detailed_metadata(announcement_url)
                detailed_metadata.update(announcement_metadata)
                detailed_metadata.update(basic_company_data)

                for attachment_url in attachment_urls:

                    try:
                        # Here we check that our datastore does not already
                        # contain a document with this URL
                        # Doing so enables us to re-run the scraper without
                        # filling the datastore with duplicates

                        id = self.check_tag(url=attachment_url)

                        # This is the line that triggers the import into our system
                        # Aleph will then download the url, store a copy,
                        # extract the text from it (doing OCR etc as needed)
                        # and index text, title and metadata
                        self.emit_url(
                            url = attachment_url,
                            title = announcement_metadata['Announcement Title'],
                            meta = detailed_metadata,
                        )

                    except TagExists:
                        pass

                

 
if __name__ == '__main__':
    crawler = ExampleCrawler()
    crawler.crawl()
    
