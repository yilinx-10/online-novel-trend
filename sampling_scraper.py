import requests                     
from bs4 import BeautifulSoup as bs 
import csv
import json
import argparse
import boto3

s3 = boto3.client('s3')
bucket_name = 'online-novel-trend'

def request_soup(url):
    r = None
    while True:
        try:               
            r = requests.get(url)
            break
        except:
            continue
    soup = bs(r.text, "html.parser")
    return soup

def select_table(soup):

    content = soup.find('table', attrs = {'class': 'cytable'}).find('tbody')
    table = content.find_all('tr')

    if len(table) < 1:
        return False

    return table

def scrape_new_books(table, url_lst):
    for entry in table[1:]:
        info = entry.find_all('td')
        # book id
        book_str = info[1].find('a').get('href').strip()
        # book link
        book_link = 'https://www.jjwxc.net/' + book_str
        url_lst.append(book_link)

def safe_scraper(page, url_lst):

    soup = request_soup(page)
    table = select_table(soup)

    if table:
        scrape_new_books(table, url_lst)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bookbase Page Scraping.")
    parser.add_argument('--input_file', type=str, required=True)
    args = parser.parse_args()

    with open(args.input_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            year = row['year']
            originality = row['originality']
            romance = row['romance']
            page = row['pagenumber'] 
            url_lst = []
            url = f"""
                    https://www.jjwxc.net/bookbase.php? \
                    fw0=0&fbsj{year}={year}& \ 
                    novelbefavoritedcount0=0&yc{originality}={originality}& \
                    xx{romance}={romance}&mainview0=0&sd0=0& \
                    lx0=0&collectiontypes=ors&notlikecollectiontypes=ors& \
                    bq=-1&removebq=&searchkeywords=&page={page}&sortType=4
                    """
            safe_scraper(url, url_lst)
            if url_lst:
                key_name = f'book_batch_{year}_{originality}_{romance}_{page}.json'
                s3.put_object(
                    Bucket=bucket_name,
                    Key=key_name, 
                    Body = json.dumps(url_lst, ensure_ascii=False, indent=2)
                )


