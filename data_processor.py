import dis
from json.decoder import JSONDecodeError
import ssl
import pydantic
from pydantic import errors
from pydantic.error_wrappers import ValidationError
import models
import asyncio
import aiohttp
from requests import get
from datetime import datetime
import re
import sched
import time
from threading import Timer

valid_articles = []
errors_list = []
current_data = set()


def clean_dates(data: dict):
    '''
    Convert the publication and modification dates
    strings to datetime object, append them to the
    original data and renames the wrongly named
    fields in O(1) i.e constant time returning the
    cleaned data.
    '''
    pub_date = data.get('pub_date', None)
    mod_date = data.get('mod_date', None)
    data['publication_date'] = datetime.strptime(pub_date,
                                                 '%Y-%m-%d-%H;%M;%S')\
        if pub_date else pub_date

    data['modification_date'] = datetime.strptime(mod_date,
                                                  '%Y-%m-%d-%H:%M:%S')\
        if pub_date else pub_date
    data.pop('pub_date')
    data.pop('mod_date')

    if not data['publication_date']:
        data['publication_date'] = datetime.now()

    return data


def add_media_and_image(section, media_data):
    '''
    Adds the image and media types directly into the section list
    and returns the modified list. it takes O(n) time to run.
    '''
    for data in media_data:
        section.append(data)
    return section


def get_data(url: str) -> dict:
    '''
    Sends http get request to any URL and returns
    the a dictionary representation of the response.
    if data doesnt exist it returns an empty list.
    Runs in O(1) time.
    '''
    # with aiohttp.ClientSession() as session:
    #     response = session.get(url, ssl=False)

    # return response.json()
    try:
        data = get(url)
        data = get(url).json() if data.status_code == 200 else []

    except JSONDecodeError:
        return None

    return data


def create_article(clean_article):
    '''
    Creates articles and tracks errors using the two global
    lists. it runs in O(n).
    '''
    try:
        article_obj = models.Article(**clean_article)
        valid_articles.append(article_obj)
    except pydantic.ValidationError as e:
        errors_list.append(e)

    return [valid_articles, errors_list]


def process_data(article_data: dict) -> None:
    # async with aiohttp.ClientSession() as session:
    for data in article_data:
        article_url = 'https://mapping-test.fra1.'\
                    f'digitaloceanspaces.com/data/articles/{data["id"]}.json'
        images_url = 'https://mapping-test.fra1.digitaloceanspaces.com'\
                     f'/data/media/{data["id"]}.json'

        article_data = get_data(article_url)
        images_data =  get_data(images_url)
        article_data['url'] = article_url
        clean_article = clean_dates(article_data)
        sections = clean_article.pop('sections')
        cleaned_sections = remove_html_tags(sections)
        images_data
        clean_article['sections'] = add_media_and_image(cleaned_sections,
                                                        images_data)
        create_article(clean_article)

    return


def remove_html_tags(sections):
    '''
    Removes all html tags from sections with text
    content and returns the modified content.It takes
    O(n) time to run.
    '''
    robj = re.compile(r'<.+?>')
    for section in sections:
        section_content = section.get('text', None)
        section['text'] = robj.sub('', section_content)\
            if section_content else section_content

    return sections


def display_result():
    '''
    Display the results of the entire process from the
    global lists of valid_articles and errors_list. It
    runs in O(1).
    '''
    print(f'\n\n\t\t\t{"*" *20}Valid Article Data{"*" *20}\n\n{valid_articles} \n\n\n')
    print(f'\t\t\t{"*" *20} Errors During Creation{"*" *20}\n\n{errors_list} \n\n\n')
    return


def get_updates(urls, current_data):
    '''
    Fetches the latest data from the API and checks if there is an
    update, it returns the new data for processing. This runs every
    Five minutes and has a time complexity of O(k), k= length of the
    latest data from the endpoint.
    '''
    delay_time = 60 * 5  # 5 minutes
    t = Timer(delay_time, get_updates, args=(urls, current_data))
    t.start()
    latest_data = get_data(urls)
    print('\nChecking for updates.....')

    if current_data != latest_data:
        print('\nUpdates found!, processing.....')
        difference = [i for i in latest_data if i not in current_data]
        process_data(difference)
        display_result()
        return

    print("No update found!")
    return


def main():
    start = time.time()
    data_url = 'https://mapping-test.fra1.'\
                            'digitaloceanspaces.com/data/list.json'
    article_data = get_data(data_url)
    process_data(article_data)
    display_result()
    end = time.time()
    print(f"Took {end - start} seconds to run")
    get_updates(data_url, article_data)


if __name__ == '__main__':
    main()
