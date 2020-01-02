if 1==1:
    from page_urls import get_number_of_pages,get_number_of_pages_from_soup
    from utility_funcs import join_urls,make_dir_if_not_exist
    from constants import *
    from bs4 import BeautifulSoup
    from page_urls import get_page_links
    import queue
    import threading
    import datetime
    import time
    import random
    import json
    import requests
    from requests.exceptions import ConnectionError
    from urllib3.exceptions import ProtocolError
    from queue import Queue
    import logging
    import os
    from itertools import islice
    import numpy
    from utility_funcs import get_request,save_file
    from constants import TOPFOLDER

TOP_FOLDER = 'raw_html_files/movies'

def make_tasks(task_queue,movie_dict):
    '''opens the dict that holds all the movies and finds out how many subpages it has, put that
    number and the link onto a task queue, the workers will figure out the links to go to'''

    for year,movies in movie_dict.items():
        # only do something in the years where there were movies
        if movies == {}:
            continue
        else:
            for movie,url in movies.items():
                url = f'{url}/user-reviews'

                completed = False

                while not completed:
                    try:
                        time.sleep(random.uniform(1,2))
                        page = requests.get(url, headers=header)
                        completed = True

                        if page.status_code == 200:
                            completed = True
                            # rate limiting
                        elif page.status_code == 429:
                            print('Rate limited! Waiting 30 secs')
                            time.sleep(30)

                    except ConnectionError:
                        time.sleep(10)
                        print('Connection error, sleeping for 5')
                    except Exception as e:
                        msg = f'Failed on {url}, {e}'
                        logging.error(msg)
                        print(msg)
                        continue


                soup = BeautifulSoup(page.content,features="html.parser")
                num_pages = get_number_of_pages_from_soup(soup)

                try:
                    # put the base url and the number of pages only the task queue
                    product_title = soup.select('.product_page_title')[0].text.strip().replace('/', '')
                    # set the save folder
                    save_folder = os.path.join(TOP_FOLDER, product_title)

                except Exception as e:
                    logging.error(f'Failed on: {url}, {e}')
                    print(f'Failed on: {url}, {e}')
                    continue


                # if it already exsists, do not need to do it
                if os.path.isdir(save_folder):
                    print(f'{save_folder} already exists, skipping')
                else:
                    print(f'Adding task pages:{num_pages} url:{url}')
                    task_queue.put( (url,int(num_pages))  )
                    time.sleep(random.uniform(1,2))

    print("Added all tasks")

def download_all_raw_from_tasks(task_queue,top_folder=TOP_FOLDER):
    '''takes tasks from the task queue and downloads the raw html of the subpages of that url'''

    while True:
        while not task_queue.empty():
            url,num_pages = task_queue.get()
            raw_subpages = []

            for i in range(num_pages+1):
                params['page'] = i
                completed = False

                while not completed:
                    try:
                        time.sleep(random.uniform(1, 2))
                        page = requests.get(url, headers=header,params=params)

                        if page.status_code==200:
                            completed = True
                        # rate limiting
                        elif page.status_code==429:
                            print('Rate limited! Waiting 30 secs')
                            time.sleep(30)

                    except ConnectionError:
                        time.sleep(10)
                        print('Connection error, sleeping for 5')
                    except ProtocolError:
                        time.sleep(10)
                        print('Connection error, sleeping for 5')
                    except Exception as e:
                        msg = f'Failed on {url}, {e}'
                        logging.error(msg)
                        print(msg)
                        continue

                # extract the soup and save it it the array, when all done write all at once
                soup = BeautifulSoup(page.content, features="html.parser")
                raw_subpages.append(soup)

            try:
                product_title = raw_subpages[0].select('.product_page_title')[0].text.strip().replace('/','')

                # set the save folder
                save_folder = os.path.join(top_folder, product_title)
                # make the folder if it is not there
                make_dir_if_not_exist(save_folder)

                for n, raw in enumerate(raw_subpages):
                    page_title = f'{product_title}_{n}'
                    file_path = os.path.join(save_folder, page_title + '.html')
                    with open(file_path, 'w') as raw_html_file:
                        raw_html_file.write(page.text)
                        print(f'Saving {file_path}')

            except Exception as e:
                print(f'Failed on {url}, {e}')
                logging.error(f'Failed on {url}, {e}')

        if task_queue.done is True:
            break
        else:
            print("Nothing to do at the moment, checking back in 10 sec")
            time.sleep(10)

    print(f'Stopping thread since task queue is done')

def slice_dict_into_in_parts(org_dict,parts):
    '''returns the provided dict as a list of dicts split in <parts> different dicts'''
    from numpy import array_split
    dict_as_list = list(org_dict.items())
    new_dicts = array_split(dict_as_list,parts)
    new_dicts = [dict(item) for item in new_dicts]
    return new_dicts

def download_raw_on_single_url(task_queue,top_folder=TOP_FOLDER):
    # TODO finish this
    while not task_queue.empty():
        url = task_queue.get()
        raw_subpages = []

        completed = False
        while not completed:
            try:
                time.sleep(random.uniform(1, 2))
                page = requests.get(url, headers=header, params=params)

                if page.status_code == 200:
                    completed = True
                # rate limiting
                elif page.status_code == 429:
                    print('Rate limited! Waiting 30 secs')
                    time.sleep(30)

            except ConnectionError:
                time.sleep(10)
                print('Connection error, sleeping for 5')
            except ProtocolError:
                time.sleep(10)
                print('Connection error, sleeping for 5')
            except Exception as e:
                msg = f'Failed on {url}, {e}'
                logging.error(msg)
                print(msg)
                continue

        # extract the soup and save it it the array, when all done write all at once
        soup = BeautifulSoup(page.content, features="html.parser")

        raw_subpages.append(soup)

    try:
        product_title = raw_subpages[0].select('.product_page_title')[0].text.strip().replace('/', '')

        # set the save folder
        save_folder = os.path.join(top_folder, product_title)
        # make the folder if it is not there
        make_dir_if_not_exist(save_folder)

        for n, raw in enumerate(raw_subpages):
            page_title = f'{product_title}_{n}'
            file_path = os.path.join(save_folder, page_title + '.html')
            with open(file_path, 'w') as raw_html_file:
                raw_html_file.write(page.text)
                print(f'Saving {file_path}')

    except Exception as e:
        print(f'Failed on {url}, {e}')
        logging.error(f'Failed on {url}, {e}')

    print(f'Stopping thread since task queue is done')


def download_raw_of_list(list_of_urls,num_workers):

    print(f'Downloading {len(list_of_urls)} urls')

    task_queue = Queue()
    task_queue.done = False

    task_consumers = []

    for url in list_of_urls:
        task_queue.put(url)

    for i in range(num_workers):
        task_consumers.append(threading.Thread(target=download_all_raw_from_tasks,args=[task_queue]))

    for worker in task_consumers:
        worker.start()

    for worker in task_consumers:
        worker.join()

    task_queue.done = True

def download_all_raw_html(num_workers):
    task_queue = Queue()
    task_queue.done = False

    with open('all_movies_with_titles_and_links_by_year.json','r') as file:
        all_movies_dict = json.loads(file.read())

    movie_dicts = slice_dict_into_in_parts(all_movies_dict,num_workers)
    task_producers = []
    task_consumers = []
    for movie_dict in movie_dicts:
        task_producers.append(threading.Thread(target=make_tasks,args=[task_queue,movie_dict]))

    for i in range(num_workers):
        task_consumers.append(threading.Thread(target=download_all_raw_from_tasks,args=[task_queue]))

    for producer in task_producers:
        producer.start()
    for worker in task_consumers:
        worker.start()
    for producer in task_producers:
        producer.join()

    for worker in task_consumers:
        worker.join()

    task_queue.done = True



if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s -%(funcName)s - %(levelname)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S',
                        level=logging.INFO, filename="log_file.log")


    save_all_subpages_of_movie('https://www.metacritic.com/movie/parasite')