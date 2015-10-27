from aleph.crawlers import Crawler, TagExists
from BeautifulSoup import BeautifulSoup
import json
from random import randint
import re
import requests
import sys
from time import sleep


SEARCH_URL = 'http://www.bseindia.com/corporates/List_Scrips.aspx'
ANNOUNCEMENTS_URL = 'http://www.bseindia.com/corporates/ann.aspx?scrip={}&dur=A&expandable=0'
ANNOUNCEMENTS_NEXT_URL = 'http://www.bseindia.com/corporates/'
ANNUAL_REPORTS_URL = 'http://www.bseindia.com/stock-share-price/stockreach_annualreports.aspx?scripcode={}&expandable=0'
INDUSTRIES = ['Aluminium', 'Carbon Black', 'Copper', 'Coal', 'Exploration & Production', 'Industrial Gases',
              'Integrated Oil & Gas', 'Iron & Steel Products', 'Iron & Steel/Interm.Products', 'Mining',
              'Oil Equipment & Services', 'Oil Marketing & Distribution', 'Other Non-Ferrous Metals', 
              'Petrochemicals', 'Refineries/ Petro-Products', 'Zinc']
STATUSES = ['Active', 'Suspended', 'Delisted']
GROUPS = ['A ', 'B ', 'D ', 'DT', 'E ', 'F ', 'I ', 'IP', 'M ', 'MT', 'P ', 'T ', 'Z ', 'ZP']
DEBUG = True
PROXIES = {}
#PROXIES = {'http': 'http://213.85.92.10:80'}

if __name__ == '__main__':
    from oocrawlers.examplecrawler import TestCrawler
    Crawler = TestCrawler


class BombayCrawler(Crawler):
    LABEL = "Bombay Stock Exchange"
    SITE = "http://www.bseindia.com/"

    def list_companies(self):
        #===== Create empty list for storing basic companies meta data =====#
        companies = []
        
        #===== Create session =====#
        session = requests.Session()
        
        #===== Set header fields for URL request =====#
        headers = {'Host': 'www.bseindia.com',
                   'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:40.0) Gecko/20100101 Firefox/40.0',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'Accept-Encoding': 'gzip, deflate',
                   'Referer': SEARCH_URL,
                   'Cookie': '__asc=618edb861501ab49d3338eb2c66; __auc=7d497d7c15004a6f5025a5f535c; _ga=GA1.2.1222465077.1443187062; _gat=1; gettabs=0; expandable=0c',
                   'Connection': 'keep-alive',
                   'Content-Type': 'application/x-www-form-urlencoded',
                   #'Content-Length': '8951',
                   }
        
        #===== Loop through all possible parameters =====#
        for industry in INDUSTRIES:
            for status in STATUSES:
                for group in GROUPS:
                    #===== Define parameters for URL request =====#
                    data = {'__VIEWSTATE': '/wEPDwUKMTY1OTcwNzY0MQ9kFgJmD2QWAgIDD2QWAgIDD2QWCAILDxAPFgYeDURhdGFUZXh0RmllbGQFCkdST1VQX0NPREUeDkRhdGFWYWx1ZUZpZWxkBQpHUk9VUF9DT0RFHgtfIURhdGFCb3VuZGdkEBUPBlNlbGVjdAJBIAJCIAJEIAJEVAJFIAJGIAJJIAJJUAJNIAJNVAJQIAJUIAJaIAJaUBUPBlNlbGVjdAJBIAJCIAJEIAJEVAJFIAJGIAJJIAJJUAJNIAJNVAJQIAJUIAJaIAJaUBQrAw9nZ2dnZ2dnZ2dnZ2dnZ2dkZAINDxAPFgYfAAUNaW5kdXN0cnlfbmFtZR8BBQ1pbmR1c3RyeV9uYW1lHwJnZBAVfAZTZWxlY3QADDIvMyBXaGVlbGVycxNBZHZlcnRpc2luZyAmIE1lZGlhCUFlcm9zcGFjZQ1BZ3JvY2hlbWljYWxzCEFpcmxpbmVzCUFsdW1pbml1bRVBc3NldCBNYW5hZ2VtZW50IENvcy4WQXV0byBQYXJ0cyAmIEVxdWlwbWVudBxBdXRvIFR5cmVzICYgUnViYmVyIFByb2R1Y3RzBUJhbmtzDUJpb3RlY2hub2xvZ3kHQlBPL0tQTxhCcmV3ZXJpZXMgJiBEaXN0aWxsZXJpZXMXQnJvYWRjYXN0aW5nICYgQ2FibGUgVFYMQ2FyYm9uIEJsYWNrF0NhcnMgJiBVdGlsaXR5IFZlaGljbGVzGENlbWVudCAmIENlbWVudCBQcm9kdWN0cxtDaWdhcmV0dGVzLFRvYmFjY28gUHJvZHVjdHMEQ29hbBhDb21tLlByaW50aW5nL1N0YXRpb25lcnkcQ29tbS5UcmFkaW5nICAmIERpc3RyaWJ1dGlvbhNDb21tZXJjaWFsIFZlaGljbGVzE0NvbW1vZGl0eSBDaGVtaWNhbHMRQ29tcHV0ZXIgSGFyZHdhcmUaQ29uc3RydWN0aW9uICYgRW5naW5lZXJpbmcWQ29uc3RydWN0aW9uIE1hdGVyaWFscxNDb25zdWx0aW5nIFNlcnZpY2VzFENvbnN1bWVyIEVsZWN0cm9uaWNzFkNvbnRhaW5lcnMgJiBQYWNrYWdpbmcGQ29wcGVyGERhdGEgUHJvY2Vzc2luZyBTZXJ2aWNlcwdEZWZlbmNlEURlcGFydG1lbnQgU3RvcmVzDERpc3RyaWJ1dG9ycwtEaXZlcnNpZmllZAtFZGlibGUgT2lscwlFZHVjYXRpb24SRWxlY3RyaWMgVXRpbGl0aWVzFUVsZWN0cm9uaWMgQ29tcG9uZW50cxhFeHBsb3JhdGlvbiAmIFByb2R1Y3Rpb24LRmVydGlsaXplcnMRRmlicmVzICYgUGxhc3RpY3MZRmluYW5jZSAoaW5jbHVkaW5nIE5CRkNzKRZGaW5hbmNpYWwgSW5zdGl0dXRpb25zFkZvb2QgJiBEcnVncyBSZXRhaWxpbmcIRm9vdHdlYXIPRm9yZXN0IFByb2R1Y3RzG0Z1cm5pdHVyZSxGdXJuaXNoaW5nLFBhaW50cxtHaWZ0IEFydGljbGVzLFRveXMgJiBDYXJkcyAVSGVhbHRoY2FyZSBGYWNpbGl0aWVzE0hlYWx0aGNhcmUgU2VydmljZXMTSGVhbHRoY2FyZSBTdXBwbGllcxpIZWF2eSBFbGVjdHJpY2FsIEVxdWlwbWVudBFIb2xkaW5nIENvbXBhbmllcwZIb3RlbHMUSG91c2Vob2xkIEFwcGxpYW5jZXMSSG91c2Vob2xkIFByb2R1Y3RzCUhvdXNld2FyZRBIb3VzaW5nIEZpbmFuY2UgEEluZHVzdHJpYWwgR2FzZXMUSW5kdXN0cmlhbCBNYWNoaW5lcnkUSW50ZWdyYXRlZCBPaWwgJiBHYXMbSW50ZXJuZXQgJiBDYXRhbG9ndWUgUmV0YWlsHEludGVybmV0IFNvZnR3YXJlICYgU2VydmljZXMUSW52ZXN0bWVudCBDb21wYW5pZXMVSXJvbiAmIFN0ZWVsIFByb2R1Y3RzHElyb24gJiBTdGVlbC9JbnRlcm0uUHJvZHVjdHMYSVQgQ29uc3VsdGluZyAmIFNvZnR3YXJlF0lUIE5ldHdvcmtpbmcgRXF1aXBtZW50FElUIFNvZnR3YXJlIFByb2R1Y3RzFElUIFRyYWluaW5nIFNlcnZpY2VzFEp1dGUgJiBKdXRlIFByb2R1Y3RzFk1hcmluZSBQb3J0ICYgU2VydmljZXMRTWVkaWNhbCBFcXVpcG1lbnQGTWluaW5nGE1pc2MuQ29tbWVyY2lhbCBTZXJ2aWNlcxZNb3ZpZXMgJiBFbnRlcnRhaW5tZW50F05vbi1hbGNvaG9saWMgQmV2ZXJhZ2VzG05vbi1EdXJhYmxlIEhvdXNlaG9sZCBQcm9kLhhPaWwgRXF1aXBtZW50ICYgU2VydmljZXMcT2lsIE1hcmtldGluZyAmIERpc3RyaWJ1dGlvbhtPdGhlciBBZ3JpY3VsdHVyYWwgUHJvZHVjdHMcT3RoZXIgQXBwYXJlbHMgJiBBY2Nlc3NvcmllcxlPdGhlciBFbGVjdC5FcXVpcC4vIFByb2QuGE90aGVyIEZpbmFuY2lhbCBTZXJ2aWNlcxNPdGhlciBGb29kIFByb2R1Y3RzFk90aGVyIEluZHVzdHJpYWwgR29vZHMZT3RoZXIgSW5kdXN0cmlhbCBQcm9kdWN0cxhPdGhlciBMZWlzdXJlIEZhY2lsaXRpZXMWT3RoZXIgTGVpc3VyZSBQcm9kdWN0cxhPdGhlciBOb24tRmVycm91cyBNZXRhbHMWT3RoZXIgVGVsZWNvbSBTZXJ2aWNlcw5QYWNrYWdlZCBGb29kcxZQYXBlciAmIFBhcGVyIFByb2R1Y3RzEVBlcnNvbmFsIFByb2R1Y3RzDlBldHJvY2hlbWljYWxzD1BoYXJtYWNldXRpY2FscxVQaG90b2dyYXBoaWMgUHJvZHVjdHMQUGxhc3RpYyBQcm9kdWN0cwpQdWJsaXNoaW5nBlJlYWx0eRpSZWZpbmVyaWVzLyBQZXRyby1Qcm9kdWN0cwtSZXN0YXVyYW50cxBSb2FkcyAmIEhpZ2h3YXlzCFNoaXBwaW5nFFNwLkNvbnN1bWVyIFNlcnZpY2VzE1NwZWNpYWx0eSBDaGVtaWNhbHMQU3BlY2lhbHR5IFJldGFpbBtTdG9yYWdlIE1lZGlhICYgUGVyaXBoZXJhbHMFU3VnYXIWU3VyZmFjZSBUcmFuc3BvcnRhdGlvbgxUZWEgJiBDb2ZmZWUcVGVsZWNvbSAtIEFsdGVybmF0ZSBDYXJyaWVycw5UZWxlY29tIENhYmxlcxFUZWxlY29tIEVxdWlwbWVudBBUZWxlY29tIFNlcnZpY2VzCFRleHRpbGVzGlRyYW5zcG9ydCBSZWxhdGVkIFNlcnZpY2VzGlRyYW5zcG9ydGF0aW9uIC0gTG9naXN0aWNzF1RyYXZlbCBTdXBwb3J0IFNlcnZpY2VzE1V0aWxpdGllczpOb24tRWxlYy4EWmluYxV8BlNlbGVjdAAMMi8zIFdoZWVsZXJzE0FkdmVydGlzaW5nICYgTWVkaWEJQWVyb3NwYWNlDUFncm9jaGVtaWNhbHMIQWlybGluZXMJQWx1bWluaXVtFUFzc2V0IE1hbmFnZW1lbnQgQ29zLhZBdXRvIFBhcnRzICYgRXF1aXBtZW50HEF1dG8gVHlyZXMgJiBSdWJiZXIgUHJvZHVjdHMFQmFua3MNQmlvdGVjaG5vbG9neQdCUE8vS1BPGEJyZXdlcmllcyAmIERpc3RpbGxlcmllcxdCcm9hZGNhc3RpbmcgJiBDYWJsZSBUVgxDYXJib24gQmxhY2sXQ2FycyAmIFV0aWxpdHkgVmVoaWNsZXMYQ2VtZW50ICYgQ2VtZW50IFByb2R1Y3RzG0NpZ2FyZXR0ZXMsVG9iYWNjbyBQcm9kdWN0cwRDb2FsGENvbW0uUHJpbnRpbmcvU3RhdGlvbmVyeRxDb21tLlRyYWRpbmcgICYgRGlzdHJpYnV0aW9uE0NvbW1lcmNpYWwgVmVoaWNsZXMTQ29tbW9kaXR5IENoZW1pY2FscxFDb21wdXRlciBIYXJkd2FyZRpDb25zdHJ1Y3Rpb24gJiBFbmdpbmVlcmluZxZDb25zdHJ1Y3Rpb24gTWF0ZXJpYWxzE0NvbnN1bHRpbmcgU2VydmljZXMUQ29uc3VtZXIgRWxlY3Ryb25pY3MWQ29udGFpbmVycyAmIFBhY2thZ2luZwZDb3BwZXIYRGF0YSBQcm9jZXNzaW5nIFNlcnZpY2VzB0RlZmVuY2URRGVwYXJ0bWVudCBTdG9yZXMMRGlzdHJpYnV0b3JzC0RpdmVyc2lmaWVkC0VkaWJsZSBPaWxzCUVkdWNhdGlvbhJFbGVjdHJpYyBVdGlsaXRpZXMVRWxlY3Ryb25pYyBDb21wb25lbnRzGEV4cGxvcmF0aW9uICYgUHJvZHVjdGlvbgtGZXJ0aWxpemVycxFGaWJyZXMgJiBQbGFzdGljcxlGaW5hbmNlIChpbmNsdWRpbmcgTkJGQ3MpFkZpbmFuY2lhbCBJbnN0aXR1dGlvbnMWRm9vZCAmIERydWdzIFJldGFpbGluZwhGb290d2Vhcg9Gb3Jlc3QgUHJvZHVjdHMbRnVybml0dXJlLEZ1cm5pc2hpbmcsUGFpbnRzG0dpZnQgQXJ0aWNsZXMsVG95cyAmIENhcmRzIBVIZWFsdGhjYXJlIEZhY2lsaXRpZXMTSGVhbHRoY2FyZSBTZXJ2aWNlcxNIZWFsdGhjYXJlIFN1cHBsaWVzGkhlYXZ5IEVsZWN0cmljYWwgRXF1aXBtZW50EUhvbGRpbmcgQ29tcGFuaWVzBkhvdGVscxRIb3VzZWhvbGQgQXBwbGlhbmNlcxJIb3VzZWhvbGQgUHJvZHVjdHMJSG91c2V3YXJlEEhvdXNpbmcgRmluYW5jZSAQSW5kdXN0cmlhbCBHYXNlcxRJbmR1c3RyaWFsIE1hY2hpbmVyeRRJbnRlZ3JhdGVkIE9pbCAmIEdhcxtJbnRlcm5ldCAmIENhdGFsb2d1ZSBSZXRhaWwcSW50ZXJuZXQgU29mdHdhcmUgJiBTZXJ2aWNlcxRJbnZlc3RtZW50IENvbXBhbmllcxVJcm9uICYgU3RlZWwgUHJvZHVjdHMcSXJvbiAmIFN0ZWVsL0ludGVybS5Qcm9kdWN0cxhJVCBDb25zdWx0aW5nICYgU29mdHdhcmUXSVQgTmV0d29ya2luZyBFcXVpcG1lbnQUSVQgU29mdHdhcmUgUHJvZHVjdHMUSVQgVHJhaW5pbmcgU2VydmljZXMUSnV0ZSAmIEp1dGUgUHJvZHVjdHMWTWFyaW5lIFBvcnQgJiBTZXJ2aWNlcxFNZWRpY2FsIEVxdWlwbWVudAZNaW5pbmcYTWlzYy5Db21tZXJjaWFsIFNlcnZpY2VzFk1vdmllcyAmIEVudGVydGFpbm1lbnQXTm9uLWFsY29ob2xpYyBCZXZlcmFnZXMbTm9uLUR1cmFibGUgSG91c2Vob2xkIFByb2QuGE9pbCBFcXVpcG1lbnQgJiBTZXJ2aWNlcxxPaWwgTWFya2V0aW5nICYgRGlzdHJpYnV0aW9uG090aGVyIEFncmljdWx0dXJhbCBQcm9kdWN0cxxPdGhlciBBcHBhcmVscyAmIEFjY2Vzc29yaWVzGU90aGVyIEVsZWN0LkVxdWlwLi8gUHJvZC4YT3RoZXIgRmluYW5jaWFsIFNlcnZpY2VzE090aGVyIEZvb2QgUHJvZHVjdHMWT3RoZXIgSW5kdXN0cmlhbCBHb29kcxlPdGhlciBJbmR1c3RyaWFsIFByb2R1Y3RzGE90aGVyIExlaXN1cmUgRmFjaWxpdGllcxZPdGhlciBMZWlzdXJlIFByb2R1Y3RzGE90aGVyIE5vbi1GZXJyb3VzIE1ldGFscxZPdGhlciBUZWxlY29tIFNlcnZpY2VzDlBhY2thZ2VkIEZvb2RzFlBhcGVyICYgUGFwZXIgUHJvZHVjdHMRUGVyc29uYWwgUHJvZHVjdHMOUGV0cm9jaGVtaWNhbHMPUGhhcm1hY2V1dGljYWxzFVBob3RvZ3JhcGhpYyBQcm9kdWN0cxBQbGFzdGljIFByb2R1Y3RzClB1Ymxpc2hpbmcGUmVhbHR5GlJlZmluZXJpZXMvIFBldHJvLVByb2R1Y3RzC1Jlc3RhdXJhbnRzEFJvYWRzICYgSGlnaHdheXMIU2hpcHBpbmcUU3AuQ29uc3VtZXIgU2VydmljZXMTU3BlY2lhbHR5IENoZW1pY2FscxBTcGVjaWFsdHkgUmV0YWlsG1N0b3JhZ2UgTWVkaWEgJiBQZXJpcGhlcmFscwVTdWdhchZTdXJmYWNlIFRyYW5zcG9ydGF0aW9uDFRlYSAmIENvZmZlZRxUZWxlY29tIC0gQWx0ZXJuYXRlIENhcnJpZXJzDlRlbGVjb20gQ2FibGVzEVRlbGVjb20gRXF1aXBtZW50EFRlbGVjb20gU2VydmljZXMIVGV4dGlsZXMaVHJhbnNwb3J0IFJlbGF0ZWQgU2VydmljZXMaVHJhbnNwb3J0YXRpb24gLSBMb2dpc3RpY3MXVHJhdmVsIFN1cHBvcnQgU2VydmljZXMTVXRpbGl0aWVzOk5vbi1FbGVjLgRaaW5jFCsDfGdnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dkZAIRDxYCHgdWaXNpYmxlaGQCEw88KwANAGQYAgUeX19Db250cm9sc1JlcXVpcmVQb3N0QmFja0tleV9fFgEFI2N0bDAwJENvbnRlbnRQbGFjZUhvbGRlcjEkYnRuU3VibWl0BSBjdGwwMCRDb250ZW50UGxhY2VIb2xkZXIxJGd2RGF0YQ9nZHF+r4IBp8W8370LWLsdEkGRj+KC',
                            '__VIEWSTATEGENERATOR': 'CF507786',
                            '__EVENTVALIDATION': '/wEWmQECg8SDjQIC/J6elwMCvuzHng4C2MiawQwC/c+kowMCwozr0wYCguj9yA0CxZa2wg8C2fPMrAICz5CzgA4C7vvawAIC0L+KiQsC64z9gQkC+/CRlAoChNPKtAcChdPKtAcCh9PKtAcCh9O6qQcCgNPKtAcCgdPKtAcCvNPKtAcCvNOKqQcCuNPKtAcCuNO6qQcCq9PKtAcCt9PKtAcCrdPKtAcCrdOKqQcCwtWLxAYC4pn6iQcC86qnhgkCz6iQBgLJ/K+YAQLe5v3wCwKZnauOCwL2v+XKCAKQwrb9CAKb1K9YAtS6iOcPAreDz4AKAp3hwOkPAofZt5QOArXz4zMCjpLv+w4Cl/DsowEC+tDwHAKBq6niCgLp1rCjDgLgx90uAuyQh+EMAuPl+LMFAumO8OUFAq/H2csOAuTIuC8Cy8XB+wMCiI7uqAcCuZL10AMCpe6p6gQC9P3VowcC8MPVwgkCl7SxwgUC86bz6wcCh+mM4wICws7tlQQCzoSDiwsCkNPC9gcC75/d2AcC/OrjrgMCgfyOoQsChI6V6AMCv96B5wcClZrK0gECiJmtrAICqN6CtwgChLru9Q0CmZGfTwK/q/T8CwLy7+/VAwL938DSDgKa/c/BDwLGv934DwLGm9GzDwLe6tq+AQKs87CCCgKKmIWsBwLFyrOxCQKk84r9DALXkf++BgLuiJPUCgLw9ve4BQKrx4ZYAtbMj9AFArSknokEArL3m+YMAuqjv+IDApD1geEEAoi5tMYOAqypkJgNAq2b2OcNAr+NqeADAsfE5qsJApj+rscHAtu814UDAv/7ybUOAvmEi8EJAtug4RcC4MPX3AcCnI+KqAkCgLWRvAsCn/vbtAQC2u7VqgYC2YTV4gsC0PHs5AUC7O/fvwcCwfLMzAECreiY0goCoanivQ4ClrbhvgUC4+3WhA4CmqTlngwC3IrJ8gcCgbysigwCl+KutgkC76unuAoCxqej+w0Cg+uNxwYC4eOmxg8CvsPC1AkCzOKfHgKZ8cKjAgL/xfzlBQKzt/GMCgKg28mJBAK+/OrKCAKOy4jsAgK+1LvHDALZw8mZDQKexp/DBgKHh5bTAgK6yIO4AwKYjo78DALii73uBgLgyaPHBALHzOy1AQLGktX/BQLkwryIBgLSj9eiDgKux/WfCALZg5j4DwLBos3gAgLM0+WgCwKou6DgDAL40JWiCtowv+Cs+GotEC5lpJg1zXPVtfkJ',
                            'myDestination': '#',
                            'WINDOW_NAMER': '1',
                            'ctl00$ContentPlaceHolder1$hdnCode': "",
                            'ctl00$ContentPlaceHolder1$ddSegment': "Equity",
                            'ctl00$ContentPlaceHolder1$ddlStatus': "{}".format(status),
                            'ctl00$ContentPlaceHolder1$getTExtData': "",
                            'ctl00$ContentPlaceHolder1$ddlGroup': "{}".format(group),
                            'ctl00$ContentPlaceHolder1$ddlIndustry': "{}".format(industry),
                            'ctl00$ContentPlaceHolder1$btnSubmit.x': "{}".format(randint(1, 60)),
                            'ctl00$ContentPlaceHolder1$btnSubmit.y': "{}".format(randint(1, 25))}
                    
                    #===== Create request for getting page with list of companies for given search query =====#
                    try:
                        response = session.post(SEARCH_URL, headers=headers, data=data, proxies=PROXIES)
                    except Exception as error:
                        print(error)
                        sys.exit()
                    
                    #===== Server returned page =====#
                    if response.status_code == 200:
                        
                        #===== Create soup =====#
                        soup = BeautifulSoup(response.text)
                        
                        #===== Find table containing data =====#
                        data_table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_gvData'})              
                        
                        #===== Try to find rows if table exists =====#
                        if data_table:
                            rows = data_table.findAll('tr', {'class': None})
                            
                            #===== Loop through rows =====#
                            for row in rows:
                                
                                #===== Find columns with data =====#
                                columns = row.findAll('td')
                                
                                #===== Try to get data =====#
                                try:
                                
                                    #===== Get company name and create dictionary for basic company data =====#
                                    company_name = columns[2].text
                                    if DEBUG:
                                        print('Scraping data for {} in group {}, industry {}'.format(company_name, group, industry))
                                    basic_company_data = {}
                                    
                                    #===== Get data =====#
                                    basic_company_data['Company Name'] = company_name
                                    basic_company_data['security_code'] = columns[0].text
                                    basic_company_data['company_url'] = columns[0].find('a')['href']
                                    basic_company_data['security_id'] = columns[1].text
                                    basic_company_data['status'] = columns[3].text
                                    basic_company_data['group'] = columns[4].text
                                    basic_company_data['face_value'] = columns[5].text
                                    basic_company_data['isin_no'] = columns[6].text
                                    basic_company_data['industry'] = columns[7].text
                                    basic_company_data['instrument'] = columns[8].text
                                    
                                #===== Some error, skip entry =====#
                                except Exception as error:
                                    if DEBUG:
                                        print(industry, status, group)
                                        print(error)
                                    continue
                                    
                                #===== Add data to the companies list =====#
                                companies.append([company_name, basic_company_data])
                                
                        #===== Add some sleep to avoid blocking by server =====#
                        sleep(randint(1, 20) / 10.0)
                        
                    #===== Server didn't return the page, show message and exit =====#
                    else:
                        print('Error getting page for group {}, industry {}, status {}.'.format(group, industry, status))
            
        print('Found {} companies.'.format(len(companies)))
         
        #===== Return list of company URLs with basic metadata =====#
        return companies

    def announcements_for_company(self, security_code, referer):
        #===== Create list for storing data =====#
        announcements = []
        
        #===== Create session =====#
        session = requests.Session()
        
        #===== Set header fields for URL request =====#
        headers = {'Host': 'www.bseindia.com',
                   'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:40.0) Gecko/20100101 Firefox/40.0',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'Accept-Encoding': 'gzip, deflate',
                   'Referer': referer,
                   'Cookie': 'redirectInfo=%7b%24%24type%3a%22RedirectInfo%22%2cc_PrevRedirect%3anull%2cServerEventID%3a%22560d0fb5-31e4-0000-361c-01038a838c29%22%2cTo%3a%22P6PFwhl9UDfDIfe8ymaO5M4yNVzdGeAjvtCI28EHdsActvo7AzXP8mUYzT6HSb6bcyPcPzlt5h4JAoChUp17qQsIL6j%5c%5cx2bEISr82wCMNOI8edGlPtLjV8LbUVDI4xSxROXCydlo7yGEvwZI9%5c%5cx2fMxC%5c%5cx2b6HA%5c%5cx3d%5c%5cx3d%22%2cFrom%3a%22h4bIBxWloJc2IxYc1jJjuUuF44Q%5c%5cx2b%5c%5cx2bM9At3p5vapDy0RcOKFYm%5c%5cx2buE1I5JAzRKY98Jo7X2fqRskIC%5c%5cx2f5YwVudfnh%5c%5cx2b225XaattBdPFrTiK31EbCTn4P9qOnc7kWXBgW2DH%5c%5cx2bV%22%2cc_ServerTime%3a391%2cStartDateTicks%3a1443716607020%2cEndDateTicks%3a1443716607411%2cc_Summary%3anull%2cT9%3a-1%2cTime%3a%222015-10-01T16%3a23%3a27.020%22%7d; __auc=7d497d7c15004a6f5025a5f535c; _ga=GA1.2.1222465077.1443187062; __asc=f5a462c3150242aabdade2b295c; gettabs=0; expandable=1c; __utma=253454874.1222465077.1443187062.1443716156.1443716155.1; __utmb=253454874.17.10.1443716156; __utmc=253454874; __utmz=253454874.1443716156.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _csm_ux_data=; statstabs=0; detabs=0; marktestat=0; _gat=1; __utmt=1',
                   'Connection': 'keep-alive',
                   }
                   
        #===== Create URL for first announcements page =====#
        url = ANNOUNCEMENTS_URL.format(security_code)
        
        #===== Loop until no Next >> on the page =====#
        while True:
        
            #===== Create request for getting announcement page =====#
            try:
                response = session.get(url, headers=headers, proxies=PROXIES)
            except:
                break
            
            #===== If page retrieved successfuly, process it =====#
            if response.status_code == 200:
            
                if DEBUG:
                    print('Scraping data from {}'.format(url))
                    
                #===== Create soup =====#
                soup = BeautifulSoup(response.text)
                
                #===== Find table with data (if not found, create empty string, and get to the end of this function) =====#
                table = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblann'}).find('table')
                if not table:
                    table = ''
                
                #===== Get all rows =====#
                rows = table.findAll('tr')
                
                #===== Create dict for announcements metadata =====#
                announcement_metadata = {}
                
                #===== Loop through the rows =====#
                for row in rows[1:]:
                
                    #===== Find columns in current row =====#
                    columns = row.findAll('td')
                    
                    #===== Skip row if no columns =====
                    if not columns:
                        continue
                        
                    #===== If class=announceheader, field contains filing date =====#
                    if columns[0]['class'] == 'announceheader':
                        filing_date = columns[0].text
                    
                    #===== If there are 4 columns, get filing type, title and announcement URL =====#    
                    elif len(columns) == 4:
                        announcement_metadata['filing_type'] = columns[1].text.replace('&nbsp;', '')
                        announcement_metadata['title'] = columns[0].text.replace('&nbsp;', '')
                        try:
                            announcement_url = columns[2].find('a')['href']
                        except:
                            announcement_url = ''
                    
                    #===== If there is text in column, it is summary =====#        
                    elif columns[0]['class'] == 'TTRow_leftnotices' and columns[0].text.replace('&nbsp;', '') != '':
                        announcement_metadata['summary'] = columns[0].text
                    
                    #===== If no text in column, this is separator, add data to announcements list and clear dict =====#    
                    elif columns[0]['class'] == 'TTRow_leftnotices' and columns[0].text.replace('&nbsp;', '') == '':
                        announcement_metadata['filing_date'] = filing_date
                        
                        #===== If announcement URL exists, add data to list, else skip this announcement =====#
                        if announcement_url:
                            announcements.append([announcement_url, announcement_metadata])
                            
                        announcement_metadata = {}
                
                #===== Try to find link to next page, if not found, break while True loop =====#
                try:        
                    next_url = soup.find('a', text=re.compile('Next >>')).parent['href']
                    url = ANNOUNCEMENTS_NEXT_URL + next_url
                    
                except:
                    break
                    
            #===== If not, show error page =====#
            else:
                print(response.text)
                break
                
        #===== Append annual reports =====#
        announcements += self.annual_reports_for_company(security_code, referer)
        
        #===== Return announcements list =====#
        return announcements
        
    def annual_reports_for_company(self, security_code, referer):
        #===== Create list for storing data =====#
        annual_reports = []
        
        #===== Create session =====#
        session = requests.Session()
        
        #===== Set header fields for URL request =====#
        headers = {'Host': 'www.bseindia.com',
                   'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:40.0) Gecko/20100101 Firefox/40.0',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'Accept-Encoding': 'gzip, deflate',
                   'Referer': referer,
                   'Cookie': 'redirectInfo=%7b%24%24type%3a%22RedirectInfo%22%2cc_PrevRedirect%3anull%2cServerEventID%3a%22560d0fb5-31e4-0000-361c-01038a838c29%22%2cTo%3a%22P6PFwhl9UDfDIfe8ymaO5M4yNVzdGeAjvtCI28EHdsActvo7AzXP8mUYzT6HSb6bcyPcPzlt5h4JAoChUp17qQsIL6j%5c%5cx2bEISr82wCMNOI8edGlPtLjV8LbUVDI4xSxROXCydlo7yGEvwZI9%5c%5cx2fMxC%5c%5cx2b6HA%5c%5cx3d%5c%5cx3d%22%2cFrom%3a%22h4bIBxWloJc2IxYc1jJjuUuF44Q%5c%5cx2b%5c%5cx2bM9At3p5vapDy0RcOKFYm%5c%5cx2buE1I5JAzRKY98Jo7X2fqRskIC%5c%5cx2f5YwVudfnh%5c%5cx2b225XaattBdPFrTiK31EbCTn4P9qOnc7kWXBgW2DH%5c%5cx2bV%22%2cc_ServerTime%3a391%2cStartDateTicks%3a1443716607020%2cEndDateTicks%3a1443716607411%2cc_Summary%3anull%2cT9%3a-1%2cTime%3a%222015-10-01T16%3a23%3a27.020%22%7d; __auc=7d497d7c15004a6f5025a5f535c; _ga=GA1.2.1222465077.1443187062; __asc=f5a462c3150242aabdade2b295c; gettabs=0; expandable=1c; __utma=253454874.1222465077.1443187062.1443716156.1443716155.1; __utmb=253454874.17.10.1443716156; __utmc=253454874; __utmz=253454874.1443716156.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _csm_ux_data=; statstabs=0; detabs=0; marktestat=0; _gat=1; __utmt=1',
                   'Connection': 'keep-alive',
                   }
                   
        #===== Create request for getting annual reports page =====#
        try:
            response = session.get(ANNUAL_REPORTS_URL.format(security_code), headers=headers, proxies=PROXIES)
        except Exception as e:
            print(e)
            return annual_reports
        
        #===== If page retrieved successfuly, process it =====#
        if response.status_code == 200:
        
            if DEBUG:
                print('Scraping data from {}'.format(ANNUAL_REPORTS_URL.format(security_code)))
                
            #===== Create soup =====#
            soup = BeautifulSoup(response.text)
            
            #===== Find table with data =====#
            table = soup.find('td', {'id': 'ctl00_ContentPlaceHolder1_annualreport'}).find('table')
            if not table:
                table = ''
                
            #===== Get all rows =====#
            rows = table.findAll('tr')
            
            #===== Loop through the rows =====#
            for row in rows[1:]:
            
                #===== Create dict for annual reports metadata =====#
                annual_reports_metadata = {}
            
                #===== Find columns in current row =====#
                columns = row.findAll('td')
                
                #===== Skip row if no columns =====
                if not columns:
                    continue
                    
                #===== Get data =====#
                annual_report_url = columns[1].find('a')['href']
                annual_reports_metadata['filing_date'] = columns[0].text.replace('&nbsp;', '')
                annual_reports_metadata['filing_type'] = 'Annual Report'
                annual_reports_metadata['title'] = annual_reports_metadata['summary'] = annual_reports_metadata['filing_type'] + ' ' + annual_reports_metadata['filing_date']
                
                #===== Add data to annual reports list =====#
                annual_reports.append([annual_report_url, annual_reports_metadata])
                print([annual_report_url, annual_reports_metadata])
                
        #===== If not, show error page =====#
        else:
            print(response.text)
            
        #===== Return annual reports list =====#
        return annual_reports
    
    def crawl(self):
        for company_name, basic_company_data in self.list_companies():   
            for announcement_url, announcement_metadata in self.announcements_for_company(basic_company_data['security_code'], basic_company_data['company_url']):         
                detailed_metadata = {}
                detailed_metadata.update(announcement_metadata)
                detailed_metadata.update(basic_company_data)
 
                print(detailed_metadata)
                try:
                    # Here we check that our datastore does not already
                    # contain a document with this URL
                    # Doing so enables us to re-run the scraper without
                    # filling the datastore with duplicates
                    
                    id = self.check_tag(url=announcement_url)

                    # This is the line that triggers the import into our system
                    # Aleph will then download the url, store a copy,
                    # extract the text from it (doing OCR etc as needed)
                    # and index text, title and metadata
                    self.emit_url(
                        url = announcement_url,
                        title = announcement_metadata['title'],
                        meta = detailed_metadata,
                    )

                except TagExists:
                    pass

if __name__ == '__main__':
    bc = BombayCrawler('test')
    bc.crawl()
