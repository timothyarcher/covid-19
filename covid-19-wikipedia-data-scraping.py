# Recoveries data for COVID-19 is limited and requires capturing data from a large number of sources
# Some people have been reporting the recoveries for about 6 states on Wikipedia
# The Wikipedia archive pages can be used to capture the historical recovery data
# This code goes through the Wikipedia pages and scrapes the recoveries and other data and saves it to a CSV file

#INSTRUCTIONS
# 1 You must have a folder "USAF-COVID-19/WIKIPEDIA/[state_abbr]/DAILY/" where [state_abbr] matches the state_abbr (e.g. NY for New York) set in the variable below
# 2 Set the variables below to define the state of the US from which the data will be pulled (note: I think this only works for US data right now)
# 3 Paste in the URL for the main article page that contains data in the right format (see below)
# 4 Note: The wikipedia page must have a table with the latest COVID-19 with columns for county, cases, deaths and recoveries
# 5 Run all cells and the script will create individial files in the "USAF-COVID-19/WIKIPEDIA/[state_abbr]/DAILY/"
# Fill in the metadata for the data you want to collect

import pandas as pd
import numpy as np
import math
from datetime import datetime, date, timedelta
import requests
import urllib
from bs4 import BeautifulSoup
import csv
import re
import os.path

# Function to scrape a wikipedia page to capture the COVID-19 data
# The table must have confirmed cases, deaths and recoveries by county for a given state
def scrape_wiki_pages (wiki_soup, state, state_abbr, hdate):

    # Get the table metadata to be used below
    # Check to see if the table contains td tags with values in the unique list of county names for the given state
    countiesdf = pd.read_csv('USAF-COVID-19/REFERENCE/us-counties.csv')
    counties_list = countiesdf[countiesdf.Province == state].Region.tolist()

    search_terms = ['COVID-19','County','Cases','Confirmed Cases','Deaths','Recoveries','Recovered']

    tmd = {'found':False} #Dictionary to hold the table meta data with the following dictionary values:
    # county_col: Contains the index to the county column (e.g. 0, 1, 2, 3) based on which hr tag contains the word County | county 
    # cases_col: Contains the index to the cases column (e.g. 0, 1, 2, 3) based on which hr tag contains the word Cases | cases | Confirmed | confirmed
    # deaths_col: Contains the index to the deaths column (e.g. 0, 1, 2, 3) based on which hr tag contains the word Deaths | deaths
    # recovered_col: Contains the index to the recoveries column (e.g. 0, 1, 2, 3) based on which hr tag contains the word Recoveries | recoveries | Recovered | recovered
    # county_match: True if the table contains one of the counties in the given state
    # score: Contains a count of the non-zero / non-false values in the other columns where 6 is a perfect match

    county_set = ['County','county','Parish','parish','Region','region']
    cases_set = ['Cases','cases','Confirmed Cases','confirmed cases','Confirmed_Cases','confirmed_cases','Confirmed cases','Total Cases','Positive Cases','Confirmed Cases*','Total Confirmed Cases','ConfirmedCases']
    deaths_set = ['Deaths','deaths']
    recovered_set = ['Recovered','recovered','Recoveries','recoveries','Recov.']

    tbls = wiki_soup.find_all('table', {'class' : ['wikitable sortable','wikitable sortable collapsible','wikitable plainrowheaders sortable']})
    tbl_pos = 0 #Capture the position of the table on the page, i.e. is it the first one, second one, etc.

    # Loop through all the tables and create metrics
    for tbl in tbls:
        score = 0 #Score for each table, reset the score back to zero each time we start checking a new table
        col_pos = 0 #Column position counter assuming column 0 is the first instance with text in our county_set
        county_col = 0
        cases_col = 0
        deaths_col = 0
        recovered_col = 0
        start_row = 0
        num_counties = 0

        # Get the header ('th') tags for this table
        headings = tbl.find_all('th')
        hloop = 0
        for h in headings:
            # if 'County' in headings[hloop].text:
            # any(x in teststr for x in conditions)
            # Check to see if the hr value equals (not contains) one of the values in our set (contains can lead to selecting the wrong tag)
            if any(x == h.text.rstrip().replace('<br/>',' ').replace('<br>',' ').replace('   ',' ').replace('  ',' ') for x in county_set):
                county_col = col_pos
                col_pos += 1
                score += 1 #One point if we find the county column
            elif any(x == h.text.rstrip().replace('<br/>',' ').replace('<br>',' ').replace('   ',' ').replace('  ',' ') for x in cases_set):
                cases_col = col_pos
                col_pos += 1
                score += 1 #One point if we find the county column
            elif any(x == h.text.rstrip().replace('<br/>',' ').replace('<br>',' ').replace('   ',' ').replace('  ',' ') for x in deaths_set):
                deaths_col = col_pos
                col_pos += 1
                score += 1 #One point if we find the county column
            elif any(x == h.text.rstrip().replace('<br/>',' ').replace('<br>',' ').replace('   ',' ').replace('  ',' ') for x in recovered_set):
                recovered_col = col_pos
                col_pos += 1
                score += 1 #One point if we find the county column

        # Get all the table rows
        trs = tbl.find_all('tr')
        num_rows = 0 #Counter for the total number of iterations through the loop
        county_matched = False
        for tr in trs:
            # Get the data in each row to try to find one of the expected counties in our list
            if num_rows > 0: #Skip the first row because it is likely to be the header row and many states have a county by the same name
                tds = tr.find_all(['th','td'])
                for t in tds:
                    if any(x in t.text.rstrip() for x in counties_list):
                        if county_matched == False:
                            score += 1 #One point if we find any td containing a valid county name
                            start_row = num_rows #Set the starting row for the data to the index to the first row containing data
                            county_matched = True #Only increment the score one time
                        num_counties += 1 #Increment the counter for each iteration through the loop to keep up with the current row number
                    elif county_matched == True: #If we already went through the counties, then count the footers at the end
                        break #If we found the counties earlier then we must have reached the end, so exit the loop  
            num_rows += 1

        if score == 5: #We found all 5 elements we were seeking so no need to keep going
            num_counties = num_counties - 1 #Adjust for the zero offset
            tmd = {'found':True,'tbl_pos':tbl_pos,'start_row':start_row,'num_counties':num_counties,'county_col':county_col,'cases_col':cases_col,'deaths_col':deaths_col,'recovered_col':recovered_col}
            break
        else:
            tbl_pos += 1 #Increment the table position if we don't find a match on this iteration

    if (tmd['found']):
        # Get the table rows
        tableRows = tbls[tmd['tbl_pos']].find_all('tr')
        #Get the number of rows
        footer_rows = 0 #TODO: We need to automatically figure out the number of footer rows
        dd = {} #Dictionary to hold the data scraped from the table
        i = 0 #Counter for the dictionary of records

        # Loop through the table and extract the data into a data frame
        for row in range(tmd['start_row'],tmd['start_row']+tmd['num_counties']):
            # Get the cells in the row
            cells = tableRows[row].find_all(['th','td'])
            # Exctract the cell data into a new object
            if any(x in cells[tmd['county_col']].text.rstrip() for x in counties_list): #Only get data for cells with valid counties
                dd[i] = {'Date':hdate,'Region':re.sub(r"[\(\[].*?[\)\]]", "", cells[tmd['county_col']].text.rstrip()),'Province':state,'Country':'US','Total_Confirmed':cells[tmd['cases_col']].text.rstrip().replace(',', ''),'Total_Deaths':cells[tmd['deaths_col']].text.rstrip().replace(',', ''),'Total_Recovered':cells[tmd['recovered_col']].text.rstrip().replace(',', '')}
                i += 1
        return dd
        
 # This function retreives all of the COVID-19 confirmed cases, deaths and recoveries
# by county for the given state and writes it to CSV files for each day
def scrape_state (mainurl, state, state_abbr):

    # STEP 1: Get the most recently reported data from the main article
    hdate = date.today()

    #Get the HTML data from the main webpage (i.e. currently reported data)
    html = requests.get(mainurl).content
    soup = BeautifulSoup(html, 'html.parser')

    #Call the scrape function to get the data for today
    dd1 = scrape_wiki_pages (soup, state, state_abbr, date.today())
    
    # Create an empty dataframe
    df1 = pd.DataFrame(columns=['Date','Region','Province','Country','Total_Confirmed','Total_Deaths','Total_Recovered'])
    
    # Read the dictionary into the dataframe
    df1 = pd.DataFrame.from_dict(dd1,orient='index')
    
    # Fill in the total deaths and recoveries data with zeros
    df1.Total_Deaths = pd.to_numeric(df1.Total_Deaths, errors='coerce').fillna(0)
    df1.Total_Recovered = pd.to_numeric(df1.Total_Recovered, errors='coerce').fillna(0)
    
    # Write the data from the main page to a file
    df1.to_csv('COVID-19/WIKIPEDIA/' + state_abbr + '/DAILY/' + datetime.now().strftime('%Y-%m-%d') + '-COVID-19-WIKI-' + state_abbr + '.csv',index=False)
    
    #STEP 2: Get the last revision for each date in the wikipedia revision history given the url for the main article

    #Get the wikipeida artical name from the url of the main article page (not archived)
    article_name = re.sub('^(.*[\\\/])','',mainurl)

    revision_history_url = 'https://en.wikipedia.org/w/index.php?title=' + article_name + '&offset=&limit=5000&action=history'
    html = requests.get(revision_history_url).content
    revision_history_soup = BeautifulSoup(html, 'html.parser')

    # Get the list or archive records from the Wikipedia archives page
    changelist = revision_history_soup.find_all('a', {'class' : 'mw-changeslist-date'})
    #Setup a dictionary to contain the articles
    all_articles = {}

    # newrow = pd.DataFrame(columns=['Date','ID'])
    fullhistorydf = pd.DataFrame(columns=['Date','ID'])

    i = 0 #index for the new dictionary records
    for a in changelist:    
        all_articles[i] = {'Date':datetime.strptime(a.text, '%H:%M, %d %B %Y').strftime('%Y-%m-%d'),'ID':re.search('(?<=oldid\=)\d*',a['href']).group(0)}
        i += 1

    # Create a temporary dataframe containing all of the dates and archive IDs 
    fullhistorydf = pd.DataFrame.from_dict(all_articles,orient='index')

    # Convert the date to a datetime so it sorts by date, not by string
    fullhistorydf.Date = pd.to_datetime(fullhistorydf.Date)

    # Get a list of unique dates in the archive
    unique_dates = pd.DataFrame(fullhistorydf.Date.unique())
    unique_dates = unique_dates.rename(columns={0:"Date"})
    # Convert to datetime so it sorts by date, not by string
    unique_dates.Date = pd.to_datetime(unique_dates.Date)
    #unique_dates = unique_dates[unique_dates.Date < '2020-03-22']

    # Loop through and get a dictionary of archive numbers to find the last revision of each day
    pages_dict = {}
    i = 0
    for index, d in unique_dates.iterrows():
        t = fullhistorydf[fullhistorydf.Date == d.Date]
        t = t.sort_values(by=['ID'],ascending=False)
        t = t.reset_index(drop=True)    
        #Only take the first record for each day
        pages_dict[i] = {'Date':t.iat[0,0],'ID':t.iat[0,1]}
        i += 1

    # Create a dataframe from the dictionary of archive page dates and article IDs
    pagesdf = pd.DataFrame.from_dict(pages_dict,orient='index')
    existing_file_count = 0
    # Iterate through the archived pages and capture the historical data
    for index, page in pagesdf.iterrows():
        # Don't bother to get the old data if we already downloaded it
        fn = 'COVID-19/WIKIPEDIA/' + state_abbr + '/DAILY/' + page.Date.strftime('%Y-%m-%d') + '-COVID-19-WIKI-' + state_abbr + '.csv'
        if not os.path.exists(fn):
            # If we already downloaded a couple of files then assume we have all the old ones
            # This helps avoid scraping old pages that did not have the data in the format we need
            if existing_file_count < 3:
                # Construct the URL and request the html
                page_url = 'https://en.wikipedia.org/w/index.php?title=' + article_name + '&oldid=' + page.ID
                html = requests.get(page_url).content
                soup = BeautifulSoup(html, 'html.parser')

                # Call the function to scrape the data off this specific page
                dd1 = scrape_wiki_pages (soup, state, state_abbr, page.Date)

                # Check to see if there was any data returned for the page
                if dd1 is not None and len(dd1) != 0:
                    df1 = pd.DataFrame.from_dict(dd1,orient='index')
                    df1.Date = pd.to_datetime(df1.Date)
                    df1.Total_Deaths = pd.to_numeric(df1.Total_Deaths, errors='coerce').fillna(0)
                    df1.Total_Recovered = pd.to_numeric(df1.Total_Recovered, errors='coerce').fillna(0)
                    #Write one file for each day with the date in the dataset and the filename
                    df1.to_csv(fn,index=False)
        else:
            # Keep up with if we downloaded the files in the past
            existing_file_count += 1
            
  # Setup the list of states that have recoveries data in a valid format on wikipedia
states_list = []

# Use individual appends just to keep the code readable
states_list.append(['South Dakota','SD','https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_South_Dakota'])
states_list.append(['Illinois','IL','https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_Illinois'])
states_list.append(['Maine','ME','https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_Maine'])
states_list.append(['New Jersey','NJ','https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_New_Jersey'])
states_list.append(['New York','NY','https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_New_York_(state)'])
states_list.append(['Maryland','MD','https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_Maryland'])

# Loop through the list of states and scrape all the data to individual files for each day
for state, state_abbr, url in states_list:
    print('Scraping ' + state + '...')
    scrape_state (url, state, state_abbr)

# Merge the individual daily files into a single file

# We will only check back to 22 January 2020
start_date = date(2020,1,22)
end_date = date.today()
period = end_date - start_date
datelist = []

# Create a list of dates
for dindex in range(period.days):
    currentdate = start_date + timedelta(days=dindex)
    datelist.append(currentdate.strftime('%Y-%m-%d'))

# Create an empty dataframe to contain the combined data
df2 = pd.DataFrame(columns=['Date', 'Region', 'Province', 'Country', 'Total_Confirmed', 'Total_Deaths', 'Total_Recovered'])

# loop through the list of states to retreive the data for each day
for state, state_abbr, url in states_list:
    # For each state, loop through the individual files and append them to the merged file
    for d in datelist:
        fn = 'COVID-19/WIKIPEDIA/' + state_abbr + '/DAILY/' + d + '-COVID-19-WIKI-' + state_abbr + '.csv'
        # Check if the file exists
        if os.path.exists(fn):
            # Read the data and combine it to the new dataframe
            tempdf = pd.read_csv(fn)
            df2 = df2.append(tempdf)

# Write the combined data to disk
df2.to_csv('COVID-19/WIKIPEDIA/' + date.today().strftime('%Y-%m-%d') + '-COVID-19-WIKIPEDIA.csv',index=False)
df2.to_csv('COVID-19/WIKIPEDIA/covid-19-wikipedia-recoveries.csv',index=False) # For uploaded to GitHub
