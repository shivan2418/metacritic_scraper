import requests
import time
from bs4 import BeautifulSoup
import  logging
import os

import concurrent

from scraper_funcs import get_last_page

logging.basicConfig()

all_games_by_date = 'https://www.metacritic.com/browse/games/release-date/available/pc/date/'
all_movies_by_date = 'https://www.metacritic.com/browse/movies/release-date/theaters/date'

all_games_base_url = 'https://www.metacritic.com/browse/games/release-date/available/pc/date?page=0'

header = {'User-Agent': 'Mozilla/5.0 (Macintosh'}
i = 0

all_games_folder = os.path.join('raw_files','list_of_all_games')

base_url='https://www.metacritic.com'

def get_list_all_urls(start_url):
    last = get_url_of_last_page(start_url)
    last = last.split('=')[0]
    return [f'{base_url}{last}={i}' for i in range(0,int(get_page_number(last)))]

def get_page_number(url):
    if '?' not in url:
        return 0
    else:
        return url.split('?')[-1].split('=')[-1]

def get_url_of_last_page(start_url):
    # get number of pages
    r = requests.get(start_url, headers=header)
    soup = BeautifulSoup(r.text, 'html.parser')
    # find the numbers in the bottom
    last_page = get_last_page(soup)
    return last_page

def scrape_and_save_all_pages(start_url,save_folder,file_name=None):

    if file_name is None:
        file_name = save_folder

    all_urls = get_list_all_urls(start_url)

    last_page= get_url_of_last_page(start_url)



    while True:
        url = f'{base_url}{last_page}'
        try:
                # make the request
                r = requests.get(url, headers=header)

                if r.status_code == 200:

                    with open(os.path.join(all_games_folder,f'{file_name}{get_page_number(url)}'),'w+') as file:
                        file.write(r.text)
                        print(f'Saved {url}')

                soup = BeautifulSoup(r.text,'html.parser')
                last_page = soup.select('a[rel="prev"]')[0].attrs['href']

                if len(last_page)>0:
                    time.sleep(0.5)
                else:
                    break

        except Exception as e:
            logging.error(e)
            time.sleep(0.5)
            continue


#     # find the last page
#     url = f'https://www.metacritic.com/browse/games/release-date/available/pc/date?page={i}'
#
#     try:
#
#         # make the request
#         r = requests.get(url, headers=header)
#
#         if r.status_code == 200:
#             with open(os.path.join(all_games_folder,f'{url}.html'),'w+') as file:
#                 file.write(r.text)
#                 print(f'Saved {url}')
#
#
#         soup = BeautifulSoup(r.text,'html-parser')
#         next_link  =soup.select('a[rel="next"]')
#
#         if len(next_link)>0:
#
#             time.sleep(0.5)
#             i+=1
#         else:
#             break
#
#     except Exception as e:
#         logging.error(e)
#         i+=1
#         time.sleep(0.5)
#         continue


if __name__ == '__main__':
    scrape_and_save_all_pages(all_games_by_date,'last_of_all_games')