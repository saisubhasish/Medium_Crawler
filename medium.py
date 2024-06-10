import os
import json
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient, DESCENDING as DESC
from chromedriver_loader import ChromeLoader
from dotenv import load_dotenv, find_dotenv

from logger import logger

load_dotenv(find_dotenv())

# Connect to MongoDB
try:
    client = MongoClient(os.environ.get("MONGO_URI"))
    db = client["medium_data"]
    collection = db["articles"]
    collection.create_index([("article_index", DESC)])
except Exception as e:
    print(f"Error in MongoDB connection: {str(e)}")

# Load JSON data
with open(r"json_data/medium_useful_links.json") as f:    # [ {"link_text": "Your link text", "link_url": "Your link url"},]
    json_data = json.load(f)

# Prepare URLs for loading
urls = [item['link_url'] + "/latest" for item in json_data]

# Load documents
loader = ChromeLoader(urls)
documents = loader.load()

print(f"Loaded {len(documents)} documents...")

# Function to parse HTML using BeautifulSoup
def parse_html(doc):
    logger.info("Parsing article...\n\n")
    return BeautifulSoup(doc.page_content, 'html.parser')

def get_article_data(index, soup):
    logger.info('Getting article data\n\n')
    articles = soup.find_all('div', {"class": "ab cm"})
    logger.info(f"Found {len(articles)} articles, with context: {articles}\n\n")
    data = []
    for art_index, article in enumerate(articles):
        try:
            logger.info(f"Processing article: {article}\n\n")
            logger.info(f"Processing article at index {index+1}.{art_index+1}...\n\n")
            link = article.find('div', {'role':"link"}).get('data-href') 

            article_data = {
            "title": article.find("h2").text if article.find("h2") else None,
            "date": datetime.now().strftime('%Y-%m-%d'),  # Current date
            "url": link,
            "content": scrape_article_content(link),
            "created_at": article.find('div', {'data-testid': "storyPublishDate"}).text if article.find('div', {'data-testid': "storyPublishDate"}) else None,
            "image_url": article.find("picture", {'class': "bg lo mt c"}).get('src') if article.find("picture", {'class': "bg lo mt c"}) and article.find("picture", {'class': "bg lo mt c"}).find('img') else None,
            }
            data.append(article_data)
            logger.info(f"Article data: {article_data}\n\n")
            # collection.insert_one(article_data)
        except Exception as e:
            logger.info(f"Error in article parsing at index {index+1}.{art_index+1}: {str(e)}\n\n")
            try:
                article_data = {
                "title": 'Error in article parsing!',
                "date": datetime.now().strftime('%Y-%m-%d'),  # Current date
                "url": link,
                "content": 'Error in article parsing!',
                "created_at": article.find('div', {'data-testid': "storyPublishDate"}).text if article.find('div', {'data-testid': "storyPublishDate"}) else None,
                "image_url": article.find("picture", {'class': "bg lo mt c"}).get('src') if article.find("picture", {'class': "bg lo mt c"}) and article.find("picture", {'class': "bg lo mt c"}).find('img') else None,
                }
                logger.info(f"Article data: {article_data}\n\n")
                # collection.insert_one(article_data)
            except Exception as e:
                pass
    with open('json_data/final_data.json', 'w') as f:
        json.dump(data , f)

def scrape_article_content(article_url):
    """
    Scrapes the full content of an individual Medium article.
    """
    response = requests.get(article_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    logger.info(f'Beutified article data: {soup}\n\n')

    content = ""
    for para in soup.find_all('p'):
        # logger.info(f"Scrapped paragraph: {para}\n\n")
        content += para.text + "\n"

    logger.info(f'Scrapped article content: {content}\n\n')
    return content

# Push data to MongoDB after each iteration
with concurrent.futures.ThreadPoolExecutor() as executor:
    logger.info("Execution started...\n\n")
    futures = [executor.submit(get_article_data, index, parse_html(doc)) for index, doc in enumerate(documents)]
    concurrent.futures.wait(futures)
    logger.info("Data pushed to MongoDB...\n\n")

# Close MongoDB connection
client.close()