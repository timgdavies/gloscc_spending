# This is a template for a Python scraper on morph.io (https://morph.io)
# including some code snippets below that you should find helpful

import scraperwiki
from bs4 import BeautifulSoup
from contextlib import closing
import requests
import csv
import time
#
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

print("Scraping list")
html = scraperwiki.scrape("http://www.gloucestershire.gov.uk/council-and-democracy/performance-and-spending/spend-over-500/")

soup = BeautifulSoup(html, 'html.parser')

for a in soup.find_all('a'):
    if '.csv' in a.get('href'):
        url = "http://www.gloucestershire.gov.uk" + a.get('href')
        # url = "http://localhost:8090/" + a.get('href').split("/")[3] # For local debugging
        print("Fetching "+ url)
        try:
            with closing(requests.get(url, stream=True)) as r:
                f = (line.decode('utf-8') for line in r.iter_lines())
                header_reader = csv.reader(f)
                headers = next(header_reader)
                if(headers[0] != 'Service Area'): # Handle the case when the header row is missing - making a best guess
                    headers = ['Service Area', 'BVA COP', 'Service Devison', 'Service Division Code', 'Expense Type', 'Expense Code', 'Payment Date', 'Transaction No', 'Payment Amount', 'Capital/Revenue', 'Supplier Name']
                for i in range(0,len(headers)): # Tidy up the soup of different headers we sometimes get
                    headers[i] = headers[i].lower().replace("1","").replace("tax number","company number").replace("/","").replace(".","").replace("&","").replace("  "," ").replace(" ","_").replace("number_","number").replace("capital_revenue","capitalrevenue").replace("revenue_capital","capitalrevenue").replace("devison","division")
                reader = csv.DictReader(f, delimiter=',', quotechar='"',fieldnames=headers)
                for row in reader:
                    row['transaction_ref'] = row['transaction_no'] + " - " + row["expense_code"] ## We get duplicate transaction numbers, but seem to get unique with transaction_no + expense_code
                    try: # remove any blanks
                        del(row[''])
                    except Exception:
                        pass
                    try:
                        scraperwiki.sqlite.save(unique_keys=['transaction_ref'],data=row,table_name='data')
                    except Exception as e:
                        print(e)
                        print("Failed to save row")
        except Exception as e:
            print(e)
            print "Failed to convert "+ url

        time.sleep(1)

