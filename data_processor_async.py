from json.decoder import JSONDecodeError
from aiohttp import ClientSession as session
from aiohttp.client_exceptions import ContentTypeError
import asyncio
from requests import get
import time
from datetime import datetime
from pydantic import errors
from pydantic.error_wrappers import ValidationError
import pydantic
import re
import models
from threading import Timer

BASE_ARTICLE_URL = 'https://mapping-test.fra1.\
                   digitaloceanspaces.com/data/articles/'
BASE_MEDIA_URL = 'https://mapping-test.fra1.digitaloceanspaces.com/data/media/'
BASE_DATA_URL = 'https://mapping-test.fra1.digitaloceanspaces.com/data/list.json'
valid_articles = []
errors_list = []


def get_article_tasks(session):
    '''
    creates a list of article and image tasks
    to be asynchronously sent over the network
    and return them to the caller
    '''
    article_task = []
    image_tasks = []
    data_url = BASE_DATA_URL
    try:
        article_data = get(data_url).json()
    except JSONDecodeError as e:
        raise JSONDecodeError(e)

    for index in range(len(article_data)):
        article_url = BASE_ARTICLE_URL.replace(' ', '') +\
                      f'{article_data[index]["id"]}.json'
        images_url = BASE_MEDIA_URL.replace(' ', '') +\
            f'{article_data[index]["id"]}.json'

        try:
            resp = session.get(article_url, ssl=False)
            article_task.append(resp)

        except Exception:
            article_task.append([])

        try:
            resp = session.get(images_url, ssl=False)
            image_tasks.append(resp)

        except Exception:
            image_tasks.append([])

    return [article_task, image_tasks, article_data]


def clean_dates(data: dict):
    '''
    Convert the publication and modification dates
    strings to datetime object, append them to the
    original data and renames the wrongly named
    fields in O(1) i.e constant time returning the
    cleaned data.
    '''
    # for datum in data:
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
    print(f'\n\n\t\t\t{"*" *40}Valid Article Data{"*" *40}\n\n\n')
    for data in valid_articles:
        print(f"\n\t\t\t{'*' *10} article id {data.id}{'*' *10}\n{data}\n")

    print(f'\n\n\t\t\t{"*" *20} Errors During Creation{"*" *20}\n\n')
    for error in errors_list:
        print(f"\n{error}")
    return


def get_data(url: str = BASE_MEDIA_URL) -> dict:
    '''
    Sends http get request to any URL and returns
    the a dictionary representation of the response.
    if data doesnt exist it returns an empty list.
    Runs in O(1) time.
    '''
    try:
        data = get(url)
        data = get(url).json() if data.status_code == 200 else []

    except JSONDecodeError:
        return None

    return data


async def get_updates(current_data):
    '''
    Fetches the latest data from the API and checks if there is an
    update, it returns the new data for processing. This runs every
    Five minutes and has a time complexity of O(k), k= length of the
    latest data from the endpoint.
    '''
    delay_time = 60 * 5  # 5 minutes
    t = Timer(delay_time, get_updates, args=(current_data))
    t.start()
    latest_data = get_data(BASE_DATA_URL)
    print('\nChecking for updates.....')

    if current_data != latest_data:
        print('\nUpdates found!, processing.....')
        new_data = [i for i in latest_data if i not in current_data]
        await process_data(new_data)
        display_result()
        return

    print("\nNo update found!")
    return


def add_media_and_image(section, media_data):
    '''
    Adds the image and media types directly into the section list
    and returns the modified list. it takes O(n) time to run.
    '''
    for data in media_data:
        section.append(data)
    return section


def create_article(clean_article):
    '''
    Creates articles and tracks errors using the two global
    lists. it runs in O(n).
    '''
    try:
        article_obj = models.Article(**clean_article)
        valid_articles.append(article_obj)
    except pydantic.ValidationError:
        try:
            clean_article['sections'] = None
            article_obj = models.Article(**clean_article)
            valid_articles.append(article_obj)
        except pydantic.ValidationError as e:
            errors_list.append(e)

    return [valid_articles, errors_list]


async def process_data():
    '''
    This coroutine creates a session and network request tasks
    clean the data and creates the articles
    '''
    articles_result = []
    images_result = []
    async with session() as cs:
        articles, images, article_data = get_article_tasks(cs)
        article_responses = await asyncio.gather(*articles)
        image_responses = await asyncio.gather(*images)
        for task in range(len(article_responses)):
            try:
                articles_result.append(await article_responses[task].json())
                articles_result[task]['url'] = \
                    BASE_ARTICLE_URL.replace(' ', '') +\
                    f'{articles_result[task]["id"]}.json'
                clean_article = clean_dates(articles_result[task])
                sections = clean_article.pop('sections')
                cleaned_sections = remove_html_tags(sections)

            except ContentTypeError:
                continue

            try:

                images_result.append(await image_responses[task].json())

            except ContentTypeError:
                clean_article['sections'] = None
                images_result.append([])

            new_section = add_media_and_image(
                                    cleaned_sections,
                                    images_result[task]
                                    )
            clean_article['sections'] = new_section
            create_article(clean_article)
    display_result()
    await get_updates(article_data)


def main():
    asyncio.run(process_data())


if __name__ == '__main__':
    main()
