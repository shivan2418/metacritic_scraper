import os
import requests
from constants import header
import time
import logging
from requests.exceptions import ConnectionError
from constants import TOPFOLDER
import random
import datetime

def join_urls(baseurl,*args):
    baseurl = baseurl.replace('https://','')
    baseurl = baseurl.strip().replace('/','')
    url = [a for a in args]
    url.insert(0,baseurl)
    url = "/".join(url)
    url = f'https://{url}'
    return url

def make_file_name(movie_title,page_num='0',main_page=False,append_date=True):
    sep = '_'
    template_string = []
    if append_date:
        today_date = datetime.datetime.now().date()
        template_string.append(str(today_date))

    if main_page:
        template_string.append('main_page_file')

    template_string.append(movie_title.replace('/',''))
    template_string.append(page_num)

    file_name = sep.join(template_string)

    return file_name


    f'{today_date}_{main_page_file}_{movie_title}'


def make_dir_if_not_exist(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        pass

    except Exception as e:
        print(e)

def save_file(file_name,movie_title,page):
    save_folder_path = os.path.join(TOPFOLDER, movie_title.replace('/',''))
    make_dir_if_not_exist(save_folder_path)
    save_path = f'{os.path.join(save_folder_path, file_name.replace("/",""))}.html'
    # Once on that url, save it.
    print(f'Saving: {save_path}')
    with open(save_path, 'w+') as file:
        file.write(page.text)


def get_request(url,params=None,forced_wait=1):
    '''Returns the page if successful'''
    completed = False


    while not completed:
        time.sleep(random.uniform(forced_wait,forced_wait*2))
        try:
            page = requests.get(url, headers=header)
            completed = True
        except ConnectionError:
            time.sleep(10)
            print(f'Connection error, sleeping for 10 {ConnectionError.errno}')
        except Exception as e:
            logging.error(e)
            print(f'{url}:  {e}')
            return e

    if page.status_code == 200:
        return page
    elif page.status_code == 404:
        return Exception('404')

if __name__ == '__main__':

    get_request('https://www.zerohedge.com/geop/')