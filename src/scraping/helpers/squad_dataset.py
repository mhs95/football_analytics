# Web scraping
from bs4 import BeautifulSoup
import requests

# Data manipulation
import pandas as pd
import numpy as np

# Database
from sqlalchemy import create_engine


def create_squad_dataset(table_id, links, team_names):

    squad_df = pd.DataFrame()

    for link, team_name in zip(links, team_names):

        fbref_link = requests.get(link).text
        soup = BeautifulSoup(fbref_link, "lxml")

        # Create a soup object for the all-stats table
        table = soup.find("table", id=table_id)

        # Get the column names from the first row of the table
        cols = []

        # Append team_name to the columns
        cols.append("team_name")

        # Get player name column from the row header
        cols.append(table.tbody.tr.th.get("data-stat"))

        # Get other column titles from the specific table cells
        for cell in table.tbody.tr.find_all("td"):
            cols.append(cell.get("data-stat"))

        # Create empty dataframe with column names from created list
        squad_data = pd.DataFrame(columns=cols)

        # Iterate over all the rows in the table, and add to empty dataframe
        for row in table.tbody.find_all("tr"):

            # Initialize empty dict for the player
            squad_dict = {}

            # Get team name from input
            squad_dict["team_name"] = team_name

            # Get the player's name from row header
            squad_dict["player"] = row.th.get("csk")

            # Iterate over all the cells and get the remaining column values
            for cell in row.find_all("td"):
                stat = cell.get("data-stat")
                if cell.get("data-stat") != "matches":
                    value = cell.text
                    squad_dict[stat] = value
                else:
                    link = "https://fbref.com" + cell.a.get("href")
                    squad_dict[stat] = link

            # Append player row to dataframe
            squad_data = squad_data.append(squad_dict, ignore_index=True)

        squad_df = squad_df.append(squad_data, ignore_index=True)

    return squad_df


def clean_squad_dataset(dataset):

    # Correct age variable
    dataset["age"] = dataset["age"].str[0:2]

    # Correct nationality variable
    dataset["nationality"] = dataset["nationality"].str.split().str[-1]

    # Replace empty space with missing values
    dataset = dataset.replace(r"^\s*$", np.nan, regex=True)

    # Except first 3 columns, all columns should be numerical
    for col in dataset.columns:
        if col not in ["team_name", "player", "nationality", "position"]:
            dataset[col] = dataset[col].astype(float, errors="ignore")

    return dataset


def save_squad_dataset(
    dataset, data_type, hostname, database, username, password, port
):

    engine = create_engine(
        f"postgresql://{username}:{password}@{hostname}:{port}/{database}"
    )

    dataset.to_sql(
        f"{data_type}_squad", engine, schema="public", if_exists="replace", index=False
    )
