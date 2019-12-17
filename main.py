from get_all_reviews import *
from constants import *
from save_raw import *
from page_urls import get_page_links
import queue
import threading
import time
import random


def download_raw_data(q):
    while not q.empty():
        url = q.get()
        save_raw_page(url)
        time.sleep(random.uniform(0.1,.75))
        q.task_done()

# save all raw html from this site

def save_all_raw_from_url(url,workers=5):

    all_links = get_page_links(url)
    q = queue.Queue()

    for link in all_links:
        q.put(link)

    num_workers = workers

    for i in range(num_workers):
        worker = threading.Thread(target=download_raw_data,args=[q])
        print(f'Starting worker {i}')
        worker.start()



if __name__ == '__main__':
    url = 'https://www.metacritic.com/movie/black-christmas-2019/user-reviews?sort-by=date&num_items=100?page=0'
    save_all_raw_from_url(url)
