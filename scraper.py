# This is a template for a Python scraper on morph.io (https://morph.io)
# including some code snippets below that you should find helpful

import scraperwiki
from bs4 import BeautifulSoup
from contextlib import closing
import requests
import csv
#
# # Read in a page
html = scraperwiki.scrape("http://foo.com")
#
# # Find something on the page using css selectors
# root = lxml.html.fromstring(html)
# root.cssselect("div[align='left']")
#
# # Write out to the sqlite database using scraperwiki library
# scraperwiki.sqlite.save(unique_keys=['name'], data={"name": "susan", "occupation": "software developer"})
#
# # An arbitrary query against the database
# scraperwiki.sql.select("* from data where 'name'='peter'")

# You don't have to do things with the scraperwiki and lxml libraries.
# You can use whatever libraries you want: https://morph.io/documentation/python
# All that matters is that your final data is written to an SQLite database
# called "data.sqlite" in the current working directory which has at least a table
# called "data".

html = scraperwiki.scrape("http://www.gloucestershire.gov.uk/council-and-democracy/performance-and-spending/spend-over-500/")

soup = BeautifulSoup(html, 'html.parser')

for a in soup.find_all('a'):
    if '.csv' in a.get('href'):
        url = "http://www.gloucestershire.gov.uk" + a.get('href')
        print("Fetching "+ url)
        # csvdata = scraperwiki.scrape("http://www.gloucestershire.gov.uk/" + a.get('href'))
        
        with closing(requests.get(url, stream=True)) as r:
            f = (line.decode('utf-8') for line in r.iter_lines())
            reader = csv.DictReader(f, delimiter=',', quotechar='"')
            for row in reader:
                row['hash'] = hash(frozenset(row.items()))
                try: 
                    del(row[''])
                except Exception:
                    pass
                try:
                    scraperwiki.sqlite.save(unique_keys=['hash'],data=row,table_name='data')
                except Exception:
                    print("Failed to save row")

