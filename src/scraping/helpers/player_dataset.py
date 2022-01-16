# Web scraping
from bs4 import BeautifulSoup
import requests

# Data manipulation
import pandas as pd
import numpy as np

# Database
from sqlalchemy import create_engine


def create_player_dataset(table_id, links, team_names):

    all_player_df = pd.DataFrame()

    for link, team_name in zip(links, team_names):

        # Connect to FBref page
        fbref_link = requests.get(link).text
        soup = BeautifulSoup(fbref_link, "lxml")

        # Create a soup object for the all-stats table
        table = soup.find("table", id=table_id)

        # Create list to get all player links
        player_links = []
        for row in table.tbody.find_all("tr"):
            for cell in row.find_all("td"):
                if cell.get("data-stat") == "matches":
                    link = "https://fbref.com" + cell.a.get("href")
            player_links.append(link)

        team_player_data = pd.DataFrame()

        # Loop through the links in the player_links list and extract info we need
        for link in player_links:

            individual_player_data = pd.DataFrame()

            # First browse overall stats page
            player_page = requests.get(link).text
            soup = BeautifulSoup(player_page, "lxml")

            # Get the div that contains the premier league filter
            player_prem_div = soup.find("div", class_="filter")

            # Loop through each filter and get the link for the filter that corresponds to the premier league
            for filt in player_prem_div.find_all("div", class_=""):
                if filt.a.text.strip() == "2021-2022 Premier League":
                    player_prem_link = "https://fbref.com" + filt.a.get("href")

            # Connect to premier league player stats page
            player_prem_page = requests.get(player_prem_link).text
            soup = BeautifulSoup(player_prem_page, "lxml")

            # Get player name from the player page
            player_name = soup.find("h1", itemprop="name").span.text

            # Create table object to parse through player stats
            table = soup.find("table", id="matchlogs_11160")

            # Loop through each match stats and add to dataframe
            for row in table.tbody.find_all("tr"):

                # Initialize empty dict for the player
                player_dict = {}

                # Create a team name column
                player_dict["team_name"] = team_name

                # Get the name from the previously stored variable
                player_dict["name"] = player_name

                # Get the match date from row header
                player_dict["date"] = row.th.get("csk")

                # Iterate over all the cells and get the remaining column values
                for cell in row.find_all("td"):
                    if cell.get("data-stat") != "match_report":
                        stat = cell.get("data-stat")
                        value = cell.text
                        player_dict[stat] = value

                # Append player row to individual player dataframe
                individual_player_data = individual_player_data.append(
                    player_dict, ignore_index=True
                )

            # Append individual player data to team-player data
            team_player_data = team_player_data.append(
                individual_player_data, ignore_index=True
            )

        all_player_df = all_player_df.append(team_player_data, ignore_index=True)

    return all_player_df


def clean_player_dataset(dataset):

    # Convert date to date format
    dataset["date"] = pd.to_datetime(dataset["date"])

    # Strip the round variable to only keep the number
    dataset["round"] = dataset["round"].str.split().str[1]

    # Replace empty space with missing values
    dataset = dataset.replace(r"^\s*$", np.nan, regex=True)

    # Create a list for object cols
    object_cols = [
        "team_name",
        "name",
        "date",
        "dayofweek",
        "round",
        "venue",
        "result",
        "squad",
        "opponent",
        "game_started",
        "position",
        "bench_explain",
    ]

    # # Except first 3 columns, all columns should be numerical
    for col in dataset.columns:
        if col not in object_cols:
            dataset[col] = dataset[col].astype(float, errors="ignore")

    return dataset


def save_player_dataset(
    dataset, data_type, hostname, database, username, password, port
):

    engine = create_engine(
        f"postgresql://{username}:{password}@{hostname}:{port}/{database}"
    )

    dataset.to_sql(
        f"{data_type}_player", engine, schema="public", if_exists="replace", index=False
    )
