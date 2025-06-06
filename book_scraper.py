# scraping libraries
import requests                     
from bs4 import BeautifulSoup as bs 
# other libraries
import time
import re
import json
import boto3
import argparse

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

def convert_encoding(text):
    return text.encode('latin1', errors='ignore').decode('gbk', errors='ignore')

def chapter_scraper(chapter, chapter_info):
    '''
    Scrapes Chapter
    '''
    info = chapter.find_all('td')

    # chapter_vip = False
    if info[1].find('a', attrs = {'style': 'cursor:pointer'}):
        # chapter_vip = True
        return True, chapter_info

    chapter_exist = chapter.find('a', attrs = {'itemprop': 'url'})

    if not chapter_exist:
        # Locked chapter, do not add to text but still try next chapter
        return False, chapter_info
    else:
        chapter_link = chapter_exist.get('href')

    content = request_soup(chapter_link)

    chapter_text = content.find('div', attrs = {'style': 'clear:both;'}).find_all_next(string=True)
    for sentence in chapter_text:
        if sentence.startswith('²åÈëÊéÇ©'):
            break
        chapter_info += convert_encoding(sentence).strip()

    return False, chapter_info

def book_scraper(book_link, book_info):
    soup = request_soup(book_link)

    content_lst = soup.find_all('table')
    intro_table = content_lst[0]
    info_table = soup.find('ul', attrs = {'class': 'rightul'})
    chapter_table = soup.find('table', attrs = {'class': 'cytable', 'id': 'oneboolt'})
    if not chapter_table:
        return None

    id = convert_encoding(soup.find('div', attrs = {'id': 'clickNovelid'}).text).strip()
    book_info['Book ID'] = id

    author_id = convert_encoding(soup.find('div', attrs = {'id': 'authorid_'}).text).strip()
    book_info['Author ID'] = author_id
    author = convert_encoding(soup.find('span', attrs = {'itemprop': 'author'}).text).strip()
    book_info['Author'] = author    
    title = convert_encoding(soup.find('span', attrs = {'itemprop': 'articleSection'}).text).strip()
    book_info['Novel Title'] = title

    book_info['Novel Intro'] = convert_encoding(intro_table.find('div', attrs = {'id': 'novelintro'}).text).strip()
    tags = [convert_encoding(tag.text).strip() for tag in intro_table.find_all('a', attrs = {'style': 'text-decoration:none;color: red;'})]
    book_info['Tags'] = ', '.join(tags)
    desc = intro_table.find_all('span', attrs={'style': 'color:#F98C4D'})
    if desc: 
        book_info['Summary'] = convert_encoding(desc[0].text).strip()
        book_info['Motivation'] = convert_encoding(desc[1].text).strip()
    else:
        book_info['Summary'] = ''
        book_info['Motivation'] = ''    
    character_identifier = re.compile(r"character_name")
    characters = [convert_encoding(character.text).strip() for character in intro_table.find_all('div', attrs={'class': character_identifier})]
    book_info['Characters'] = ', '.join(characters)

    book_info['IP'] = ''
    book_info['Awards'] = ''
    book_info['Genre'] = ''
    book_info['Perspective'] = ''
    book_info['Update Status'] = ''
    book_info['Word Count'] = ''
    book_info['Contract Status'] = ''

    if info_table: 
        attributes = info_table.find_all('li')
        genre = convert_encoding(attributes[0].find('span', attrs={'itemprop': 'genre'}).text).strip()
        perspective = convert_encoding(attributes[1].find('span').next_sibling).strip()
        status = convert_encoding(attributes[3].find('span', attrs={'itemprop': 'updataStatus'}).text).strip()
        word = convert_encoding(attributes[4].find('span', attrs={'itemprop': 'wordCount'}).text).strip()
        ips = attributes[5].find_all('img')
        contract = convert_encoding(attributes[6].find('b').text).strip()
        awards = attributes[7].find_all('div')

    book_info['Genre'] = genre
    book_info['Perspective'] = perspective
    book_info['Update Status'] = status
    book_info['Word Count'] = word
    book_info['Contract Status'] = contract
    if ips:
        ip_list = [convert_encoding(ip.get('title')).strip() for ip in ips]
        book_info['IP'] = ', '.join(ip_list)
    if awards:
        award_list = [convert_encoding(award.text).strip() for award in awards]
        book_info['Awards'] = ', '.join(award_list)

    chapter_lst = chapter_table.find_all('tr', attrs={'itemtype': 'http://schema.org/Chapter'})

    review = chapter_table.find('span', attrs={'itemprop': 'reviewCount'})
    subscription = chapter_table.find('span', attrs={'itemprop': 'collectedCount'})
    if review and subscription:
        book_info['Review Count'] = convert_encoding(review.text).strip()
        book_info['Subscription Count'] = convert_encoding(subscription.text).strip()
    else:
        book_info['Review Count'] = ''
        book_info['Subscription Count'] = ''

    chapter_info = ''
    for chapter in chapter_lst:
        vip, chapter_info = chapter_scraper(chapter, chapter_info)
        if vip:
            break

    chapter_info = chapter_info.replace("“", '"').replace("”", '"')
    chapter_info = chapter_info.replace("‘", "'").replace("’", "'")

    book_info['Chapters'] = chapter_info

    return id

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Book Scraping.")
    parser.add_argument('--input_file', type=str, required=True)
    args = parser.parse_args()

    match = re.search(r'book_batch_(\d{4})_(\d)_(\d)_(\d+)\.json$', args.input_file)

    if match:
        year = int(match.group(1))
        originality = int(match.group(2))
        romance = int(match.group(3))
        page = int(match.group(4))
        print(f"Year: {year}, Originality: {originality}, Romance: {romance}, Set : {page}")
    else:
        raise ValueError(f"Filename format is invalid: {args.input_file}")

    with open(args.input_file) as f:
        books = json.load(f)

    for book in books:
        time.sleep(1)
        book_info = dict()
        id = book_scraper(book, book_info)

        if id:
            s3.put_object(
                Bucket=bucket_name,
                Key=f'{year}_{originality}_{romance}/{id}.json', 
                Body = json.dumps(book_info, ensure_ascii=False, indent=4)
            )

