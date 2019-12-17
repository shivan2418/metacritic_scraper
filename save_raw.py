import requests
from bs4 import BeautifulSoup
from constants import header
from utility_funcs import make_dir_if_not_exist

import os

def save_raw_page(url,top_folder='raw_html_files'):

    page = requests.get(url,headers=header)
    soup = BeautifulSoup(page.content,features="html.parser")

    product_title = soup.select('.product_page_title')[0].text.strip()

    page_title = f'{product_title}_{url.split("=")[-1]}'
    print(page_title)

    save_folder = os.path.join(top_folder,product_title)
    make_dir_if_not_exist(save_folder)

    file_path = os.path.join(save_folder,page_title+'.html')
    with open(file_path,'w') as raw_html_file:
        raw_html_file.write(page.text)

        print(f'Saved raw html to {file_path}')

if __name__ == '__main__':
    save_raw_page('https://www.metacritic.com/movie/star-wars-episode-viii---the-last-jedi/user-reviews?sort-by=date&num_items=100&page=0')