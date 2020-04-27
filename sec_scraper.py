"""
Name: sec_scraper.py
Authors: Nick Williams, Jaimin Patel, Carter King
Last Updated: 2/25/20
Script scrapes firm 13F filings from the SEC Edgar database and outputs to a pandas dataframe
"""

import os
import re  # library for regular expressions operations
import sys  # used for reading command line arguments
from bs4 import BeautifulSoup as soup
from urllib.request import urlopen
import pandas as pd

import psycopg2
import sqlalchemy
from io import StringIO # used for bulk insert of df to db





# pass in a url and returned the requested page
def extract_webpage(page_url):
    url_data = urlopen(page_url)  # opens a network object denoted by a URL for reading
    req_page = url_data.read()
    url_data.close()


    return req_page


# function returns the web pages soup
def extract_soup(comp_soup, cik):
    # start with URL of comp filing page
    # from the filings page - get the URL for the 13F documents (grabs tags for all 'documentsbutton')
    # using BS methods for searching parse trees
    try:
        comp_filing_url = 'http://www.sec.gov' + comp_soup.find('a', {'id': 'documentsbutton'})['href']
    except TypeError:
        print('comp_filing_url: The cik is probably invalid', cik)
    except:
        print('Something went wrong')

    try:
        filingsPage_soup = soup(extract_webpage(comp_filing_url), 'html.parser')
    except:
        print("filingsPage_soup: the cik is probably invalid", cik)

    # using another BS defined method and regular expression library
    try:
        file_url = ('http://www.sec.gov' + filingsPage_soup.find('a', text=re.compile(r".*\.txt$"))['href'])
    except:
        print("file_url: the cik is provably invalid", cik)

    # get the .txt file xml soup
    try:
        file_soup = soup(extract_webpage(file_url), 'xml')
    except:
        print("file_soup: the cik is probably invalid", cik)
    print('extracted soup')

    return file_soup


# Search SEC database for fund 13F documents if they exist
def fundfinder(cik):
    # specify url of web page you want to scrape (will need to concatenate link and cik)
    try:
        comp_url = ('http://www.sec.gov/cgi-bin/browse-edgar?action='
                'getcompany&CIK=' + cik + '&type=13F&dateb=&owner=exclude&count=40')
    except:
        print("fundfinder: Couldn't find this URL", cik)

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

    print("clabel is: ", all_clabel)

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

    print("rows are: ", rows)
    return rows

# function to convert pandas dataframe to postgresql database (fast bulk insert method)
# accepts a pd df as an argument
def to_database(connection, cik, dframe):

    print(dframe)
    table_name = cik
    try:
        dframe.to_sql(table_name, connection, if_exists='replace')
    except:
        print("couldn't update database")



def parse_txt():
    companyTable = {}
    cikTable = {}
    companiesFile = open("companies.txt", "r")
    companies = companiesFile.readlines()
    for line in companies:
        splitUp = line.split(': ')
        compName = splitUp[0].rstrip()
        compCIK = splitUp[1].rstrip()
        companyTable[compCIK] = compName
        cikTable[compName] = compCIK
    companiesFile.close()
    return companyTable, cikTable




def main():

    companyTable, cikTable = parse_txt()

    #cik = '0001350694'
    # establish connection to database
    try:
        engine = sqlalchemy.create_engine(("postgresql://postgres:password@localhost/CoinLogic"))
        con = engine.connect()
        print('connected to database')
    except:
        print("couldn't connect to DB")

    for cik in companyTable:
        print('Extracting 13F: ' + cik)
        # stores the search page extracted and searched by BeautifulSoup
        comp_soup = fundfinder(cik)

        # extract the soup of the given url page
        txt_soup = extract_soup(comp_soup, cik)
        print('extracted webpage')

        #create aspects of dataframe
        file_col = pullColumns(txt_soup)
        file_rows = pullRows(txt_soup, file_col)

        # combine into a dataframe
        dframe = pd.DataFrame(data=file_rows, columns=file_col)
        print(dframe)
        print("File Extraction Complete")

        to_database(con, cik, dframe)

    con.close()

    print("updated database")


if __name__ == "__main__":
    main()