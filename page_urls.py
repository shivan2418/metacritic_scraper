import requests
import time
from bs4 import BeautifulSoup

from constants import the_last_jedi
from constants import all_games_by_date,all_movies_by_date,all_games_base_url
from constants import header

def make_url(url, page_num):

    url = url.split('=')
    url[-1]=str(page_num)
    url = "=".join(url)


    return url


def get_page_links(user_reviews_url):
    '''takes the url of a movie review, user reviews sorted by date as an input and returns a list of all the links'''
    # example https://www.metacritic.com/movie/star-wars-episode-viii---the-last-jedi/user-reviews?sort-by=date&num_items=100&page=0
    page = requests.get(user_reviews_url,headers=header).content

    soup = BeautifulSoup(page)

    # check if there is even a navbar, if not there is only one page
    if soup.select('.page_nav') == []:
        return [user_reviews_url]
    else:
        # select the number of the last page
        last_page_num = soup.select('ul li.page.last_page a')[0].text


        urls = [make_url(user_reviews_url,page) for page in range(0,int(last_page_num))]

    return urls


if __name__ == '__main__':
    print(get_page_links(the_last_jedi))
    a=get_page_links('https://www.metacritic.com/movie/a-hidden-life/user-reviews?sort-by=date&num_items=100?page=0')
    print(a)