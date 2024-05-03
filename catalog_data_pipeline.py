import requests
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
from utils.helper import get_current_time
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from dotenv import load_dotenv
import os

load_dotenv()

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

    # add new columns timestamp
    cf_data["scraped_at"] = get_current_time()

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

    # fix the values change `-` to `Not Defined`
    cf_data["fandom"] = cf_data["fandom"].replace("-", "Not Defined")

    cf_data["other_fandom"] = cf_data["other_fandom"].replace("-", "Not Defined")

    # mapping and fix the values in `fandom`
    MAPPING_VALUES = {
        "Genshin": "Genshin Impact",
        "Hololive": "Vtuber",
        "Genshin impact": "Genshin Impact",
        "Honkai: Star Rail": "Honkai Star Rail",
        "genshin impact": "Genshin Impact",
        "ghibli": "Ghibli",
        "Jujutsu kaisen": "Jujutsu Kaisen",
        "VTuber": "Vtuber",
        "Holostars": "Vtuber",
        "Star Rail": "Honkai Star Rail",
        "Studio Ghibli": "Ghibli",
        "Nijisanji": "Vtuber",
        "JJK": "Jujutsu Kaisen",
        "hoyoverse": "Hoyoverse",
        "Hoyoverse(Genshin,Honkai Star Rail)": "Hoyoverse",
        "HSR": "Honkai Star Rail",
        "V-Tuber": "Vtuber",
        "Holo Production": "Vtuber",
        "HoloID": "Vtuber",
        "Hololive ID": "Vtuber",
        "Vtuber Indie": "Vtuber",
        "vtuber (niji+holo)": "Vtuber",
        "Vtuber (niji+holo)": "Vtuber",
        "NIJISANJI": "Vtuber",
        "Holostars EN": "Vtuber",
        "Maha5": "Vtuber",
        "Hoyoverse/Mihoyo": "Hoyoverse",
        "Virtual Youtuber Indonesia": "Vtuber",
        "VTuber Indie": "Vtuber",
        "Genshin (game)": "Genshin Impact",
        "JUJUTSU KAISEN": "Jujutsu Kaisen",
        "Blue Archives": "Blue Archive",
        "Streamer Vtuber": "Vtuber",
        "virtual youtuber": "Vtuber",
        "Nijisanji ID": "Vtuber",
        "Virtual youtuber": "Vtuber",
        "Ghibli Studio": "Ghibli",
        "vtuber": "Vtuber",
        "vtuber (nijisanji, vshojo)": "Vtuber",
        "honkai star rail": "Honkai Star Rail",
        "Vtuber (AISATSET ID)": "Vtuber"
    }

    cf_data["fandom"] = cf_data["fandom"].replace(MAPPING_VALUES)

    # create a list of cols that we want to arrange
    ARRANGE_COLS = ["name", "circle_code", "circle_cut", "day", "sells_commision",
                    "sells_comic", "sells_artbook", "sells_photobook_general",
                    "sells_novel", "sells_game", "sells_music", "sells_goods",
                    "sells_handmade_crafts", "sells_magazine", "sells_photobook_cosplay",
                    "circle_facebook", "circle_instagram", "circle_twitter",
                    "circle_other_socials", "marketplace_link", "fandom",
                    "other_fandom", "rating", "sampleworks_images", "scraped_at"]

    cf_data = cf_data[ARRANGE_COLS]

    return cf_data

def load_data(cf_data: pd.DataFrame) -> None:
    """
    Function yang digunakan untuk insert data ke dalam Google Spreadsheet

    Parameters
    ----------
    cf_data (pd.DataFrame): hasil transformasi data catalog
    """
    # init credentials
    GSPREAD_KEY_JSON = os.getenv("GSPREAD_KEY_JSON")
    GSPREAD_KEY_SHEET = os.getenv("GSPREAD_KEY_SHEET")
    GSPREAD_WORKSHEET_NAME = os.getenv("GSPREAD_WORKSHEET_NAME")

    scopes = ['https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive']

    credentials = Credentials.from_service_account_file(GSPREAD_KEY_JSON, scopes=scopes)

    gc = gspread.authorize(credentials)

    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)

    # open a google sheet
    gs = gc.open_by_key(GSPREAD_KEY_SHEET)
    # select a work sheet from its name
    worksheet1 = gs.worksheet(GSPREAD_WORKSHEET_NAME)
    
    # write it to spreadsheet
    worksheet1.clear()
    set_with_dataframe(worksheet=worksheet1,
                    dataframe=cf_data,
                    include_index=False,
                    include_column_header=True,
                    resize=True)

if __name__ == "__main__":
    print("====== Start Process Scraping CF Catalog ======")

    # 1. scrape the data
    url = "https://catalog.comifuro.net/"
    
    print("Start Scraping...")
    scrape_data = scrape_comifuro_data(url = url)

    # 2. transform the catalog data
    print("Start Transform Data...")
    transform_data = transform_data(cf_data = scrape_data)

    # 3. load data to spreadsheet
    print("Start Load Data to Spreadsheet...")
    load_data(cf_data = transform_data)

    print("====== End Process Scraping CF Catalog ======")