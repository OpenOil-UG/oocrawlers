from aleph.crawlers import Crawler, TagExists

from BeautifulSoup import BeautifulSoup
import json
from random import randint
import re
import requests
import sys
from time import sleep

if __name__ == '__main__':
    from oocrawlers.examplecrawler import TestCrawler
    Crawler = TestCrawler

STARTING_URL = 'https://www.jse.co.za'
COMPANIES_URL = 'https://www.jse.co.za/_vti_bin/JSE/CustomerRoleService.svc/GetAllIssuers'
COMPANY_INFO_URL = 'https://www.jse.co.za/current-companies/companies-and-financial-instruments/issuer-profile?issuermasterid={}'
NATURE_OF_BUSINESS_URL = 'https://www.jse.co.za/_vti_bin/JSE/CustomerRoleService.svc/GetIssuerNatureOfBusiness'
ASSOCIATED_ROLES_URL = 'https://www.jse.co.za/_vti_bin/JSE/CustomerRoleService.svc/GetIssuerAssociatedRoles'
INSTRUMENTS_FOR_ISSUER_URL = 'https://www.jse.co.za/_vti_bin/JSE/SharesService.svc/GetAllInstrumentsForIssuer'
ANNOUNCEMENTS_SEARCH_URL = 'https://www.jse.co.za/_vti_bin/JSE/SENSService.svc/GetSensAnnouncementsByIssuerMasterId'
REGULATORY_DOCUMENTS_URL = 'https://www.jse.co.za/_vti_bin/JSE/WebstirService.svc/GetWebstirDocumentsByIssuerMasterIdAndYear'
REGULATORY_DOCUMENTS_YEARS_URL = 'https://www.jse.co.za/_vti_bin/JSE/WebstirService.svc/GetWebstirDocumentYearsByIssuerMasterId'
#PROXIES = {'https': 'https://54.207.45.19:3333'}
PROXIES = {}
false = ""


class SouthAfricaCrawler(Crawler):
    LABEL = "Johannesburg Stock Exchange"
    SITE = "https://www.jse.co.za/"
    MAX_RESULTS = 99999


    def list_companies(self):
        #===== Create empty list for storing basic companies meta data =====#
        companies = []
        
        #===== Set header fields for URL request =====#
        headers = {'Host': 'www.jse.co.za',
                   'User-Agent': '"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:40.0) Gecko/20100101 Firefox/40.0"',
                   'Accept': 'application/json, text/javascript, */*; q=0.01',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'Accept-Encoding': 'gzip, deflate',
                   'Content-Type': 'application/json; charset=UTF-8',
                   'X-Requested-With': 'XMLHttpRequest',
                   'Referer': 'https://www.jse.co.za/current-companies/companies-and-financial-instruments',
                   'Cookie': '_ga=GA1.3.473812871.1441294880; SearchSession=2451a284-3aad-4bb0-9040-df5133b89bcc; _gat=1; WSS_FullScreenMode=false',
                   'Pragma': 'no-cache',
                   'Cache-Control': 'no-cache'}
                   
        #===== Define parameters for URL request =====#
        data = '{"filterLongName": "", "filterType": "Equity Issuer"}'
        
        #===== Create request for getting page with JSON objects containing companies data =====#
        try:
            response = requests.post(COMPANIES_URL, headers=headers, data=data, verify=True, proxies=PROXIES)
        except Exception as error:
            print(error)
            sys.exit()
        
        #===== Check if server returned page =====#
        if response.status_code == 200:
            
            #===== Get data from response (replace null with "", it throws error on eval otherwise) =====#
            try:
                data = eval(response.text.replace('null', '""'))
            except Exception as error:
                print(error)
                sys.exit()                
            
        #===== Server didn't return the page, show message and exit =====#
        else:
            print('Server didn\'t respond properly. Exiting program.')
            sys.exit()
            
        print('Found {} companies.'.format(len(data)))
         
        #===== Loop through each company and get basic data =====#   
        for company in data:
            basic_details = {}
            
            try:
                company_name = company['LongName']
                basic_details['Company Name'] = company_name
            except:
                continue
            
            try:
                basic_details['Company ID'] = company['AlphaCode']
            except:
                pass
                
            try:
                basic_details['e_mail'] = company['EmailAddress']
            except:
                pass
                
            try:
                basic_details['phone'] = company['TelephoneNumber']
            except:
                pass
            
            try:
                basic_details['fax'] = company['FaxNumber']
            except:
                pass
            
            try:
                basic_details['website'] = company['Website']
            except:
                pass
            
            try:
                basic_details['postal_address'] = company['PostalAddress']
            except:
                pass
            
            try:
                basic_details['physical_address'] = company['PhysicalAddress']
            except:
                pass
            
            try:
                basic_details['branches'] = company['Branches']
            except:
                pass
            
            try:
                basic_details['contacts'] = company['Contacts']
            except:
                pass
            
            try:
                basic_details['role_description'] = company['RoleDescription']
            except:
                pass
            
            try:
                basic_details['registration_number'] = company['RegistrationNumber']
            except:
                pass
            
            try:
                basic_details['status'] = company['Status']
            except:
                pass
            
            try:
                basic_details['master_id'] = company['MasterID']
            except:
                continue
            
            #===== Append company data to companies list =====#
            companies.append([company_name, basic_details])  
        
        #===== Return list with companies from required indestries =====#
        print('Retrieved {} companies.'.format(len(companies)))
        return companies

    def get_detailed_metadata(self, master_id):
        #===== Create variables that would contain wanted data =====#
        detailed_metadata = {}
        
        #===== Set header fields for URL request =====#
        headers = {'Host': 'www.jse.co.za',
                   'User-Agent': '"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:40.0) Gecko/20100101 Firefox/40.0"',
                   'Accept': 'application/json, text/javascript, */*; q=0.01',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'Accept-Encoding': 'gzip, deflate',
                   'Content-Type': 'application/json; charset=UTF-8',
                   'X-Requested-With': 'XMLHttpRequest',
                   'Referer': COMPANY_INFO_URL.format(master_id),
                   'Cookie': '_ga=GA1.3.473812871.1441294880; SearchSession=2451a284-3aad-4bb0-9040-df5133b89bcc; _gat=1; WSS_FullScreenMode=false',
                   'Pragma': 'no-cache',
                   'Cache-Control': 'no-cache'}
        
        #===== Define parameters for URL request =====#
        request_data = '{"issuerMasterId": "' + str(master_id) + '"}'
        
        #===== Create request for getting nature of business page =====#
        try:
            
            response = requests.post(NATURE_OF_BUSINESS_URL, headers=headers, data=request_data, verify=True, proxies=PROXIES)
            
            #===== If page returned successfuly, get details =====#
            if response.status_code == 200:
            
                #===== Try to convert data to dictionary =====#
                try:
                    data = eval(response.text.replace('null', '""'))['GetIssuerNatureOfBusinessResult']
                    detailed_metadata['nature_of_business'] = data
                except:
                    pass
                
        except:
            pass
            
        #===== Create request for getting associated roles page =====#
        try:
            
            response = requests.post(ASSOCIATED_ROLES_URL, headers=headers, data=request_data, verify=True, proxies=PROXIES)
            
            #===== If page returned successfuly, get details =====#
            if response.status_code == 200:
            
                #===== Try to convert data to dictionary =====#
                try:
                    data = eval(response.text.replace('null', '""'))['GetIssuerAssociatedRolesResult']
                    for associated_role in data:
                        detailed_metadata[associated_role['RoleDescription']] = associated_role['LongName']
                except Exception as e:
                    print(e)
                    pass
                
        except Exception as e:
            print(e)    
            pass
            
        #===== Create request for getting instruments for issuer page =====#
        try:
            
            response = requests.post(INSTRUMENTS_FOR_ISSUER_URL, headers=headers, data=request_data, verify=True, proxies=PROXIES)
            
            #===== If page returned successfuly, get details =====#
            if response.status_code == 200:
            
                #===== Try to convert data to dictionary =====#
                try:
                    data = eval(response.text.replace('null', '""'))['GetAllInstrumentsForIssuerResult']
                    for instrument in data:
                        detailed_metadata['instruments_for_issuer'] = {'code': instrument['AlphaCode'], 'name': instrument['LongName']}
                except:
                    pass
                
        except:
            pass
                    
        #===== Return obtained data =====#
        return detailed_metadata

    def announcements_for_company(self, master_id):
        #===== Create list for storing data =====#
        announcements = []
        
        #===== Set header fields for URL request =====#
        headers = {'Host': 'www.jse.co.za',
                   'User-Agent': '"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:40.0) Gecko/20100101 Firefox/40.0"',
                   'Accept': 'application/json, text/javascript, */*; q=0.01',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'Accept-Encoding': 'gzip, deflate',
                   'Content-Type': 'application/json; charset=UTF-8',
                   'X-Requested-With': 'XMLHttpRequest',
                   'Referer': STARTING_URL,
                   'Cookie': '_ga=GA1.3.473812871.1441294880; SearchSession=2451a284-3aad-4bb0-9040-df5133b89bcc; _gat=1; WSS_FullScreenMode=false',
                   'Pragma': 'no-cache',
                   'Cache-Control': 'no-cache'}
        
        #===== Define parameters for URL request =====#
        data = {"issuerMasterId": str(master_id)}
        
        #===== Create request for getting announcement page =====#
        try:
            response = requests.post(COMPANY_INFO_URL.format(master_id), headers=headers, verify=True, proxies=PROXIES)
        except:
            return announcements
        
        #===== If page retrieved successfuly, process it =====#
        if response.status_code == 200:
                
            #===== Find related documents =====#
            titles = re.findall('"Title":"(.+?)"', response.text)
            paths = re.findall('"Path":"(.+?)"', response.text)
            
            for title, path in zip(titles, paths):
                document_metadata = {}
                
                document_url = path.replace('\\u002f', '/')
                document_metadata['title'] = title
                
                announcements.append([document_url, document_metadata])
                
        #===== If not, show error page =====#
        else:
            print(response.text)
        
        #===== Set header fields for URL request =====#
        headers = {'Host': 'www.jse.co.za',
                   'User-Agent': '"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:40.0) Gecko/20100101 Firefox/40.0"',
                   'Accept': 'application/json, text/javascript, */*; q=0.01',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'Accept-Encoding': 'gzip, deflate',
                   'Content-Type': 'application/json; charset=UTF-8',
                   'X-Requested-With': 'XMLHttpRequest',
                   'Referer': COMPANY_INFO_URL.format(master_id),
                   'Cookie': '_ga=GA1.3.473812871.1441294880; SearchSession=2451a284-3aad-4bb0-9040-df5133b89bcc; _gat=1; WSS_FullScreenMode=false',
                   'Pragma': 'no-cache',
                   'Cache-Control': 'no-cache'}
        
        #===== Define parameters for URL request =====#
        data = '{"issuerMasterId": "' + str(master_id) + '"}'
        
        #===== Create request for getting announcement page =====#
        try:
            response = requests.post(ANNOUNCEMENTS_SEARCH_URL, headers=headers, data=data, verify=True, proxies=PROXIES)
        except:
            return announcements
        
        #===== If page retrieved successfuly, process it =====#
        if response.status_code == 200:
        
            #===== Try to convert data to dictionary =====#
            try:
                data = eval(response.text.replace('null', '""'))['GetSensAnnouncementsByIssuerMasterIdResult']
            except:
                return announcements
            
            #===== Loop through announcements in dictionary ======#    
            for announcement in data:
                announcement_metadata = {}
            
                #===== Try to find key (part of URL to be added to predefined URL) =====#
                try:
                    attachment_url = announcement['PDFPath'].replace('\\/', '/')
                    
                #===== No key field in current announcement, move to next =====#
                except KeyError:
                    continue
                    
                try:
                    announcement_metadata['acknowledge_date_time'] = announcement['AcknowledgeDateTime'].replace('\\/', '')
                except:
                    pass
                    
                try:
                    announcement_metadata['announcement_id'] = announcement['AnnouncementId']
                except:
                    pass
                    
                try:
                    announcement_metadata['announcement_reference_number'] = announcement['AnnouncementReferenceNumber']
                except:
                    pass
                    
                try:
                    announcement_metadata['announcement_text'] = announcement['AnnouncementText']
                except:
                    pass
                    
                try:
                    announcement_metadata['title'] = announcement['FlashHeadline']
                except:
                    pass
                    
                #===== Append data to announcements list =====#
                announcements.append([attachment_url, announcement_metadata])
                
        #===== If not, show error page =====#
        else:
            print(response.text)
        
        #===== Define parameters for URL request (regulatory documents years) =====#  
        data = '{"issuerMasterId": "' + str(master_id) + '"}'
        
        #===== Create request for getting regulatory documents page =====#
        try:
            response = requests.post(REGULATORY_DOCUMENTS_YEARS_URL, headers=headers, data=data, verify=True, proxies=PROXIES)
        except:
            return announcements
            
        #===== If page retrieved successfuly, process it =====#
        if response.status_code == 200:
        
            #===== Try to convert data to dictionary =====#
            try:
                data = eval(response.text.replace('null', '""'))['GetWebstirDocumentYearsByIssuerMasterIdResult']
            except:
                return announcements
        
        years = []     
        for year in data:
            years.append(year)
            
        #===== Define parameters for URL request (regulatory documents) =====#
        for i in years:
        
            data = '{"issuerMasterId": "' + str(master_id) + '", "year": ' + str(i) + '}'
            
            #===== Create request for getting regulatory documents page =====#
            try:
                response = requests.post(REGULATORY_DOCUMENTS_URL, headers=headers, data=data, verify=True, proxies=PROXIES)
            except Exception as e:
                print(e)
                return announcements
                
            #===== If page retrieved successfuly, process it =====#
            if response.status_code == 200:
            
                #===== Try to convert data to dictionary =====#
                try:
                    data = eval(response.text.replace('null', '""'))['GetWebstirDocumentsByIssuerMasterIdAndYearResult']
                except:
                    return announcements
                    
                #===== Loop through announcements in dictionary ======#    
                for announcement in data:
                    announcement_metadata = {}
                
                    #===== Try to find key (part of URL to be added to predefined URL) =====#
                    try:
                        attachment_url = announcement['DocumentUrl'].replace('\\/', '/')
                        
                    #===== No key field in current announcement, move to next =====#
                    except KeyError:
                        continue
                        
                    try:
                        announcement_metadata['title'] = announcement['DocumentType']
                    except:
                        pass
                        
                    try:
                        announcement_metadata['event_type'] = announcement['EventType']
                    except:
                        pass
                        
                    try:
                        announcement_metadata['submitted_date'] = announcement['SubmittedDate'].replace('\\/', '')
                    except:
                        pass

                    #===== Append data to announcements list =====#
                    announcements.append([attachment_url, announcement_metadata])
                    
            #===== If not, show error page =====#
            else:
                print(response.text)
        
        #===== Return announcements list =====#
        return announcements
    
    def crawl(self):
        results = 0
        for company_name, basic_company_data in self.list_companies():
            detailed_metadata = self.get_detailed_metadata(basic_company_data['master_id'])
            detailed_metadata.update(basic_company_data)
            for (attachment_url, announcement_metadata) in self.announcements_for_company(basic_company_data['master_id']):
                detailed_metadata.update(announcement_metadata)
                
                print(detailed_metadata)
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
                        title = detailed_metadata['title'],
                        meta = detailed_metadata,
                    )
                    results += 1
                    if results >= self.MAX_RESULTS:
                        return

                except TagExists:
                    pass
            sleep(randint(1, 5))

if __name__ == '__main__':
    ec = SouthAfricaCrawler('test')
    ec.crawl()
