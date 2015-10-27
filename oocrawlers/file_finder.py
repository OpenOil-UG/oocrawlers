#===== Define general program attributes =====#
__author__ = 'Face'
__contact__ = 'fejsov@sbb.rs'
__version__ = '1.00'


#===== Import external libraries =====#
from aleph.crawlers import Crawler, TagExists
import csv
import json
import optparse
import requests

if __name__ == '__main__':
    from oocrawlers.examplecrawler import TestCrawler
    Crawler = TestCrawler


#===== Define constants =====#
try:
    from private import API_KEY
except ImportError:
    API_KEY = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXx'


#===== Define functions =====#
def create_parser():
    """
    Create parser and define command line options.
    """
    
    #===== Create parser with program version value =====#
    parser = optparse.OptionParser(version="%prog {}".format(__version__))
    
    #===== Add option for setting extension =====#
    parser.add_option('-e',
                      dest='extension',
                      default='pdf',
                      help='extension for wanted documents (default is pdf)')
                      
    #===== Add option for setting csv file name =====#                  
    parser.add_option('-f',
                      dest='file_name',
                      default='test.csv',
                      help='name of csv file containing list of companies urls (default is test.csv)')
                      
    #===== Return created parser =====#
    return parser


#===== Define classes =====#
class BingSearch(object):
    def __init__(self,):
        """
        Initialize url template, session object and results list.
        """
        
        #===== Create URL template =====# 
        self.url = 'https://api.datamarket.azure.com/Data.ashx/Bing/SearchWeb/v1/Composite?Query=%27{}%27&$format=json&$top={}&$skip={}'
        
        #===== Create session with authentication =====#
        self.session = requests.Session()
        self.session.auth=('', API_KEY)
        
        #===== Create list that will hold results =====#
        self.results = []
        
        #===== Create variable that will hold error status =====#
        self.error = False
        
    def search(self, query):
        """
        Search Bing.com using provided query.
        """
        import pdb; pdb.set_trace()
        #===== Clear self.results (if there were some previous searches) =====#
        self.results = []
        
        #===== Create variables for navigating through result pages =====#
        results_per_page = 50
        offset = 0
        number_of_results = 9999
    
        #===== Loop through the result pages =====#
        while offset < number_of_results:
        
            #===== Get the result for given query =====#
            response = self.session.get(self.url.format(query, results_per_page, offset))
            
            #===== Server responded with data =====#
            if response.status_code == 200:
            
                #===== Convert data to dictionary =====#
                data = eval(response.text)
                
                #===== Find the number of results =====#
                number_of_results = int(data['d']['results'][0]['WebTotal'])
                
                #===== Get results from the current page =====#
                results = data['d']['results'][0]['Web']
                
                #===== Loop through the results =====#
                for result in results:
                
                    #===== Catch KeyError (some field doesn't exist in result) =====#
                    try:
                    
                        #===== Get attachment URL =====#
                        attachment_url = result['Url']
                        
                        #===== Create dictionary for storing file info and fill it =====#
                        metadata = {}
                        metadata['description'] = result['Description']
                        metadata['title'] = result['Title']
                        
                        #===== Append data to results list =====#
                        self.results.append([attachment_url, metadata])
                        
                    #===== KeyError caught, go to next result =====#
                    except KeyError:
                        import pdb; pdb.set_trace()
                        continue
                    
                #===== Increase offset to get next page =====#
                offset += results_per_page
                
            #===== Server didn't returned data, or some error occured =====#
            else:
            
                #===== Show status code and error (if available) =====#
                print(response.status_code)
                print(response.text)
                
                #===== Set error status =====#
                self.error = True
                
                #===== Exit the loop =====#
                break
                
        #===== Return list with results =====#
        return self.results

    
class FileCrawler(Crawler):
    LABEL = "Bing API file search"
    SITE = "https://api.datamarket.azure.com/"
    
    def __init__(self, file_name, extension):
        """
        Initialize file name, extension and BingSearch object.
        """
        self.file_name = file_name
        self.extension = extension
        self.bs = BingSearch()
        self.results = []
    
    def get_companies(self):
        """
        Read specified file and return list of companies with metadata.
        """
        
        #===== Create list for storing data =====#
        companies = []
        
        #===== Try to catch IO error =====#
        try:
        
            #===== Open specified file =====#
            with open(self.file_name, 'rb') as csv_file:
            
                #===== Create CSV reader =====#
                reader = csv.reader(csv_file, delimiter=',', quotechar='"')

                #===== Loop through the rows in file =====#
                for row in reader:
                
                    #===== If URL is in first cell of a row, it is a head row, get column names and go to next row =====#
                    if row[0].lower() == 'url':
                        column_names = row
                        continue
                        
                    #===== Put data in a dictionary =====#
                    company_data = {}
                    for column, column_name in zip(row, column_names):
                        company_data[column_name.lower()] = column
                        
                    #===== Add data to companies list =====#
                    companies.append(company_data)
            
        #===== IO error, specified file doesn't exist, show error =====#
        except IOError as error:
            print('{}: {}'.format(error.args[1], file_name))
            
        #===== Return list of URLs =====#
        return companies
    
    def crawl(self):
        """
        Get files from bing.com for companies in provided csv file.
        """
        
        #===== Loop through the companies provided in csv file =====#
        for company_data in self.get_companies():
        
            #===== Create search query for current company =====#
            company_url = company_data['url']
            query = 'filetype:{} site:{}'.format(self.extension, company_url)
            
            #===== Loop through found files =====#
            for attachment_url, metadata in self.bs.search(query):
            
                #===== Create dictionary which will hold data from csv file and from search result =====#
                metadata.update(company_data)
                print(attachment_url, metadata)
                
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
                        title = metadata['title'],
                        meta = metadata,
                    )

                except TagExists:
                    pass
                    
                except KeyError:
                    pass         

class BingPDFCrawler(FileCrawler):
    LABEL = "Bing API file search"
    SITE = "https://api.datamarket.azure.com/"
    file_name = '/data/search/oocrawlers/oocrawlers/sites/oil_company_sites.csv'
    extension = 'pdf'

    def __init__(self, *args, **kwargs):
        """
        Initialize file name, extension and BingSearch object.
        """
        self.bs = BingSearch()
        self.results = []


                
#===== Main part of code =====#
if __name__ == '__main__':
    #===== Create parser for command line arguments =====#
    #parser = create_parser()
    
    #===== Get command line options =====#
    #options, args = parser.parse_args()
    #extension = options.extension
    #file_name = options.file_name
    
    #===== Create FileCrawler object and run crawling =====#
    #fc = FileCrawler(file_name, extension)
    #fc.crawl()

    bc = BingPDFCrawler()
    bc.crawl()
#else:
#    file_name = '../url_samples.txt'
#    fc = FileCrawler(file_name, 'pdf')
#    fc.crawl()
