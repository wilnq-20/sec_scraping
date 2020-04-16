"""
Name: sec_scraper.py
Authors: Nick Williams, Jaimin Patel, Carter King
Last Updated: 2/25/20
Script scrapes firm 13F filings from the SEC Edgar database and outputs to a pandas dataframe
"""

import re  # library for regular expressions operations
import sys  # used for reading command line arguments
from bs4 import BeautifulSoup as soup
from urllib.request import urlopen
import pandas as pd

import psycopg2
from io import StringIO # used for bulk insert of df to db
import os




# pass in a url and returned the requested page
def extract_webpage(page_url):
    url_data = urlopen(page_url)  # opens a network object denoted by a URL for reading
    req_page = url_data.read()
    url_data.close()

    return req_page


# function returns the web pages soup
def extract_soup(comp_soup):
    # start with URL of comp filing page
    # from the filings page - get the URL for the 13F documents (grabs tags for all 'documentsbutton')
    # using BS methods for searching parse trees
    comp_filing_url = 'http://www.sec.gov' + comp_soup.find('a', {'id': 'documentsbutton'})['href']

    filingsPage_soup = soup(extract_webpage(comp_filing_url), 'html.parser')

    # using another BS defined method and regular expression library
    file_url = ('http://www.sec.gov' + filingsPage_soup.find('a', text=re.compile(r".*\.txt$"))['href'])

    # get the .txt file xml soup
    file_soup = soup(extract_webpage(file_url), 'xml')

    return file_soup


# Search SEC database for fund 13F documents if they exist
def fundfinder(cik):
    # specify url of web page you want to scrape (will need to concatenate link and cik)
    comp_url = ('http://www.sec.gov/cgi-bin/browse-edgar?action='
                'getcompany&CIK=' + cik + '&type=13F&dateb=&owner=exclude&count=40')

    # we create a beautiful soup object by passing in two args
    find_soup = soup(extract_webpage(comp_url), 'html.parser')

    # should we check for 13f existence

    return find_soup


# function accepts soup of txt and returns list of each column header and row
def pullColumns(txt):
    all_clabel = []
    info_array = txt.find_all('infoTable')
    # check for available info
    if (info_array is None):
        print('ERROR: No Data')
        sys.exit()
    # loop through and pull column labels
    for table in info_array:
        clabel = []
        for item in table.findChildren():  # findChildren() extracts a list of Tag objects that match the given criteria
            if (item.name is not None and item.string is not None):
                clabel.append(item.name)
        if (len(clabel) > len(all_clabel)):
            all_clabel = clabel

    return all_clabel


# function accepts soup and column labels and returns extracted rows
def pullRows(txt_soup, clabel):
    rows = []

    # iterate through and build list for row
    for row in txt_soup.find_all('infoTable'):
        curr = []
        for column in clabel:
            if (row.find(column) is None):
                curr.append('N/A')  # no val for column, so label NA
            else:
                curr.append(row.find(column).string)  # else, take item from column
        rows.append(curr)
    return rows

# function to convert pandas dataframe to postgresql database (fast bulk insert method)
# accepts a pd df as an argument
def to_database(dframe):
    # establish connection to database
    conn = psycopg2.connect(dbname="", user="", password="") # good/safe practice to use env variables

    # setup string buffer initialization
    data_io = StringIO()
    data_io.write(dframe.to_csv(index=None, header=None)) # write pd df as csv to buffer
    data_io.seek(0)

    # copy string buff to db, like normal file
    # open cursor to perform database ops





def main():
    # Get requested firm cik from command line
    if len(sys.argv) > 2:
        print('Invalid number of parameters. Only enter 1 cik')  # should only accept program name and company cik
        sys.exit()  # exits python

    # store cik to search for in SEC EDGAR
    cik = sys.argv[1]
    print('Extracting 13F: ' + cik)

    # stores the search page extracted and searched by BS
    comp_soup = fundfinder(cik)

    # extract the soup of the given url page
    txt_soup = extract_soup(comp_soup)

    file_col = pullColumns(txt_soup)
    file_rows = pullRows(txt_soup, file_col)

    # combine into a dataframe
    dframe = pd.DataFrame(data=file_rows, columns=file_col)

    # convert into csv file (comment out for now because handled in above func)
    #dframe.to_csv(cik, index=False)
    print("File Extraction Complete")


if __name__ == "__main__":
    main()