import scraperwiki
from bs4 import BeautifulSoup
from contextlib import closing
import requests
import csv
import time

print("Scraping list")
url = "http://www.gloucestershire.gov.uk/"
url += "council-and-democracy/performance-and-spending/spend-over-500/"
html = scraperwiki.scrape(url)
soup = BeautifulSoup(html, 'html.parser')

for a in soup.find_all('a'):
    if '.csv' in a.get('href'):
        url = "http://www.gloucestershire.gov.uk" + a.get('href')
        # For local debugging
        # url = "http://localhost:8090/" + a.get('href').split("/")[3]
        print("Fetching " + url)
        try:
            with closing(requests.get(url, stream=True)) as r:
                f = (line.decode('cp1252').encode('utf8')
                     for line in r.iter_lines())
                header_reader = csv.reader(f)
                headers = next(header_reader)
                # Handle case when header row is missing - best guess.
                if(headers[0] != 'Service Area'):
                    headers = ['Service Area', 'BVA COP', 'Service Devison',
                               'Service Division Code', 'Expense Type',
                               'Expense Code', 'Payment Date',
                               'Transaction No', 'Payment Amount',
                               'Capital/Revenue', 'Supplier Name']
                # Tidy up the soup of different headers we sometimes get.
                for i in range(0, len(headers)):
                    headers[i] = headers[i].lower().replace("1", "")\
                        .replace("tax number", "company number")\
                        .replace("/", "").replace(".", "")\
                        .replace("&", "").replace("  ", " ")\
                        .replace(" ", "_")\
                        .replace("number_", "number")\
                        .replace("capital_revenue", "capitalrevenue")\
                        .replace("revenue_capital", "capitalrevenue")\
                        .replace("devison", "division")
                    headers[i] = headers[i].decode('cp1252').encode('utf8')
                reader = csv.DictReader(f, delimiter=',', quotechar='"',
                                        fieldnames=headers)
                for row in reader:
                    # We get duplicate transaction numbers, but seem to get
                    # unique with transaction_no + expense_code.
                    row['transaction_ref'] = row['transaction_no'] + " - " + \
                        row["expense_code"]
                    for k in row:
                        row[k] = unicode(row[k].decode('cp1252'))
                    # Remove any blanks.
                    try:
                        del(row[''])
                    except Exception:
                        pass
                    try:
                        scraperwiki.sqlite.save(
                            unique_keys=['transaction_ref'],
                            data=row, table_name='data')
                    except Exception as e:
                        print(e)
                        print("Failed to save")
        except Exception as e:
            print(e)
            print "Failed to convert " + url
        time.sleep(1)
