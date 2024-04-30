#TODO: automate this shit using lambda?
#TODO: create viz / dashboard

import requests
from bs4 import BeautifulSoup
import json
import re
import pandas as pd

def scrape_comifuro_data(url: str) -> pd.DataFrame:
    """
    Function yang digunakan untuk scrape Catalog Comifuro dengan cara
    mengambil json data yang ada di dalam tag React `window.__INITIAL_STATE__`

    Parameters
    ----------
    url (str): comifuro url

    Returns
    -------
    cf_data (pd.DataFrame): hasil dari scrape data yang sudah dalam bentuk DataFrame
    """
    # get connection to comifuro
    resp = requests.get(url)

    # create scrapper object
    soup = BeautifulSoup(resp.text, "html.parser")

    # get raw data from web page
    raw_data = soup.find_all("script")[1]

    # get the json data inside the react tag
    data = re.search(r"window\.__INITIAL_STATE__=({.*});", raw_data.text).group(1)

    # convert the data to dictionary python
    dict_data = json.loads(data)

    # select the data based on requirements
    cf_data = pd.DataFrame(dict_data["circle"]["allCircle"])

    return cf_data

def transform_data(cf_data: pd.DataFrame) -> pd.DataFrame:
    """
    Function yang digunakan untuk melakukan transform data

    Parameters
    ----------
    cf_data (pd.DataFrame): hasil scraping data catalog CF

    Returns
    -------
    cf_data (pd.DataFrame): hasil catalog CF yang sudah ditransform
    """
    # drop the unnecessary columns
    DROP_COLS = ["id", "user_id"]

    cf_data = cf_data.drop(DROP_COLS, axis = 1)

    # init list of cols that what we want to rename
    RENAME_COLS = {
        "SellsCommision": "sells_commision",
        "SellsComic": "sells_comic",
        "SellsArtbook": "sells_artbook",
        "SellsPhotobookGeneral": "sells_photobook_general",
        "SellsNovel": "sells_novel",
        "SellsGame": "sells_game",
        "SellsMusic": "sells_music",
        "SellsGoods": "sells_goods",
        "SellsHandmadeCrafts": "sells_handmade_crafts",
        "SellsMagazine": "sells_magazine",
        "SellsPhotobookCosplay": "sells_photobook_cosplay"
    }

    # rename columns based on the requirements
    cf_data = cf_data.rename(columns = RENAME_COLS)

    # create a list of cols that we want to arrange
    ARRANGE_COLS = ["name", "circle_code", "circle_cut", "day", "sells_commision",
                    "sells_comic", "sells_artbook", "sells_photobook_general",
                    "sells_novel", "sells_game", "sells_music", "sells_goods",
                    "sells_handmade_crafts", "sells_magazine", "sells_photobook_cosplay",
                    "circle_facebook", "circle_instagram", "circle_twitter",
                    "circle_other_socials", "marketplace_link", "fandom",
                    "other_fandom", "rating", "sampleworks_images"]

    cf_data = cf_data[ARRANGE_COLS]

    return cf_data

def load_data(cf_data: pd.DataFrame):
    #TODO: add load data to database or google spredsheet
    
    # untuk sekarang ke csv dulu
    cf_data.to_csv("data/cf_data_v1.csv", index = False)

if __name__ == "__main__":
    print("====== Start Process Scraping CF Catalog ======")

    # 1. scrape the data
    url = "https://catalog.comifuro.net/"
    scrape_data = scrape_comifuro_data(url = url)

    # 2. transform the catalog data
    transform_data = transform_data(cf_data = scrape_data)

    # 3. load data to csv (tmp)
    load_data(cf_data = transform_data)

    print("====== End Process Scraping CF Catalog ======")