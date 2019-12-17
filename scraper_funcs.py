def get_last_page(soup):
    '''takes a soup as input and and returns false if nothing is found, otherwise returns link to last page'''

    last = soup.select('ul.pages > .last_page > .page_num')

    if len(last)>0:
        return last[0].attrs['href']
    else:
        return False