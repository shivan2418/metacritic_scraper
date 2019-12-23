import datetime
import logging
import threading
import time
from queue import Queue
from bs4 import BeautifulSoup
import re

from constants import *
from page_urls import get_number_of_pages_from_soup
from utility_funcs import get_request, save_file
from utility_funcs import make_file_name

def download_failed():
    urls = []
    url_pat = re.compile('(https?:\/\/www\.metacritic\.com\/movie\/[\w\-]+)')
    with open('failed_dls.txt','r') as file:
        for line in file:
            urls.append(  re.findall(url_pat,line)[0]  )

    movie_queue = Queue()
    subpage_queue = Queue()

def download_all(max_workers=50):

    year_queue = Queue()
    movie_queue = Queue()
    subpage_queue = Queue()

    movie_workers = []
    subpage_workers = []
    # add all years to the year queue
    for year in range(2019,1915,-1):
        year_queue.put(year)
    print('Put all years on years queue')

    producer = threading.Thread(target=put_movies_on_movie_queue, args=[year_queue, movie_queue])
    producer.start()

    while not year_queue.empty():

        if movie_queue.qsize() > 10 and threading.active_count() < max_workers:
            movie_consumer = threading.Thread(target=save_all_subpages_of_movie, args=[movie_queue, subpage_queue])
            movie_consumer.start()

            print(f'Starting new movie consumer, queue size: {movie_queue.qsize()} workers: {len(movie_workers)}')
        if subpage_queue.qsize()>=1:
            subpage_consumer = threading.Thread(target=download_subpages, args=[subpage_queue])
            subpage_consumer.start()
            print(f'Starting new subpage consumerv qsize: {subpage_queue.qsize()} workers: {len(subpage_workers)}')

        print(f'Movie queue: {movie_queue.qsize()} subpage queue: {subpage_queue.qsize()}')
        time.sleep(5)

    print('All operations finished')

def put_movies_on_movie_queue(year_queue, movie_queue):
    '''pulls from the year queue and puts tasks on the movie queue'''
    params = {'year_selected': None, 'sort': 'desc'}
    today = datetime.datetime.now().date()

    while not year_queue.empty():
        year = year_queue.get()
        print(f'Processing {year}')
        url = f'https://www.metacritic.com/browse/movies/score/metascore/year/filtered?year_selected={year}&sort=desc'
        page = get_request(url, params=None)

        # load the page with all movies in a year
        if not isinstance(page,Exception):
            soup = BeautifulSoup(page.content,features="html.parser")
            num_pages = get_number_of_pages_from_soup(soup)

            #  all the ones on the first page no matter what
            movie_urls = soup.select('.title.numbered + a')
            for url in movie_urls:
                u = f"https://www.metacritic.com{url.attrs['href']}"
                movie_queue.put(u)
            print(f'Put {len(movie_urls)} urls on movie queue')
            if int(num_pages) == 0:
                continue
            else:
                # iterate over each in the year page
                for year_page in range(1,int(num_pages)):
                    print(f'Accessing {year} page {year_page} Queue size {year_queue.qsize()}')
                    # iterate over each movie
                    url = f'https://www.metacritic.com/browse/movies/score/metascore/year/filtered?year_selected={year}&sort=desc&page={year_page}'
                    page = get_request(url, params=None)
                    soup = BeautifulSoup(page.content, features="html.parser")

                    movie_urls = soup.select('.title.numbered + a')
                    for url in movie_urls:
                        u = f"https://www.metacritic.com{url.attrs['href']}"
                        movie_queue.put(u)
                    print(f'Put {len(movie_urls)} urls on movie queue')

    print(f'Stopping due put movies on queue due to empty queue')

def download_subpages(subpage_queue):
    '''pulls tasks from  the task queue and downloads the url'''

    today = datetime.datetime.now().date()

    while not subpage_queue.empty():
        url, pagenum = subpage_queue.get()
        print(f'Getting page {pagenum} {url}')

        params['page'] = pagenum
        page = get_request(url,params=params)

        if not isinstance(page,Exception):
            soup = BeautifulSoup(page.content,features='html.parser')
            movie_title = soup.select('.product_page_title h1')[0].text
            file_name = make_file_name(movie_title,pagenum)
            save_file(file_name,movie_title,page)
        else:
            logging.error(f'Could not download single url, {url},{pagenum}')

    print(f'Stopping download subpage on queue due to empty queue')


def save_all_subpages_of_movie(movie_queue,subpage_queue):
    '''Takes the URL to a movies page as input, saves the main page and every subpage of the user review section'''
    # visit the first movie on the page and load it.

    today_date = datetime.datetime.now().date()

    while not movie_queue.empty():
        url = movie_queue.get()
        print(f'{threading.currentThread()}: Accessing {url}')

        page = get_request(url)

        if not isinstance(page,Exception):
            soup = BeautifulSoup(page.content,features='html.parser')
            movie_title = soup.select('.product_page_title h1')[0].text
            actual_url = page.url
            file_name = make_file_name(movie_title,None,True,True)
            # save main page
            save_file(file_name,movie_title,page)
            # record the actual URL that was visited
            usr_review_url = f'{actual_url}/user-reviews'
            # then visit the user-reviews section
            subpage = get_request(usr_review_url,params=params)

            # check if could access the user review section
            if not isinstance(subpage,Exception):

                try:

                    sub_soup = BeautifulSoup(subpage.content,features='html.parser')
                    num_pages = get_number_of_pages_from_soup(sub_soup)

                    # save the first page no matter what
                    file_name = make_file_name(movie_title,0)
                    save_file(file_name, movie_title, subpage)

                    # just one page, just save the page and then done
                    if int(num_pages) == 0:
                       continue

                    # if there are more than one page, put it the rest of the pages onto the queue
                    elif int(num_pages) > 0:
                        for i in range(1,int(num_pages)):
                            print('Put page on subpage queue')
                            subpage_queue.put((usr_review_url,i))
                except Exception as e:
                    logging.error(f'Error fetching sub_page {actual_url}, {subpage} {e}')

            # if an error was log that
            else:
                logging.error(f'Error fetching sub_page {actual_url}, {subpage}')
        else:
            logging.error(f'Error fetching page {url}, {page}')

    print('Shutting down due to empty queue')

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s -%(funcName)s - %(levelname)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S',
                        level=logging.INFO, filename="log_file.log")


    #download_all()
    download_failed()