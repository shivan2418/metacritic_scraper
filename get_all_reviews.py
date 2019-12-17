import json
import requests
from constants import header
from bs4 import BeautifulSoup
from bs4.element import NavigableString

def get_all_reviews_on_page(url):

    page = requests.get(url,headers=header)
    soup = BeautifulSoup(page.content)

    for review in soup.select('.review'):
        a=list(review.children)
        a = [item for item in a if not isinstance(item,NavigableString)]


        for i in a:
            print(i.text)

    return soup



if __name__ == '__main__':
    get_all_reviews_on_page('https://www.metacritic.com/movie/star-wars-episode-viii---the-last-jedi/user-reviews?sort-by=date&num_items=100&page=40')