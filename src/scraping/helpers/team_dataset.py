# Web scraping
from bs4 import BeautifulSoup
import requests

# Data manipulation
import pandas as pd
import numpy as np

# Database
from sqlalchemy import create_engine


def create_team_dataset(table_id, links, team_names):

    teams_df = pd.DataFrame()

    for link, team_name in zip(links, team_names):

        # Create soup object
        fbref_link = requests.get(link).text
        soup = BeautifulSoup(fbref_link, "lxml")

        # Find the table with team stats
        table = soup.find("table", id=table_id)

        # Get the column names from the first row of the table
        cols = []

        # Append team_name to the columns
        cols.append("team_name")

        # Get date column from the row header
        cols.append(table.tbody.tr.th.get("data-stat"))

        # Get other column titles from the specific table cells
        for cell in table.tbody.tr.find_all("td"):
            cols.append(cell.get("data-stat"))

        # Create empty dataframe with column names from created list
        team_data = pd.DataFrame(columns=cols)

        # Iterate over all the rows in the table, and add to empty dataframe
        for row in table.tbody.find_all("tr"):

            # Initialize empty dict for the team
            team_dict = {}

            # Get team name from input
            team_dict["team_name"] = team_name

            # Get the match date from row header
            team_dict["date"] = row.th.get("csk")

            # Iterate over all the cells and get the remaining column values
            for cell in row.find_all("td"):
                stat = cell.get("data-stat")
                value = cell.text
                team_dict[stat] = value

            # Append team-match row to dataframe
            team_data = team_data.append(team_dict, ignore_index=True)

            # Drop rows where the match is yet to be played
            team_data = team_data[team_data["result"] != ""]

        teams_df = teams_df.append(team_data, ignore_index=True)

    return teams_df


def clean_team_dataset(dataset):

    cols_to_keep = [
        "team_name",
        "date",
        "comp",
        "round",
        "dayofweek",
        "venue",
        "result",
        "goals_for",
        "goals_against",
        "opponent",
        "xg_for",
        "xg_against",
        "possession",
    ]

    dataset = dataset[cols_to_keep]

    # # Convert date to date format
    dataset["date"] = pd.to_datetime(dataset["date"])

    # Keep only premier league games
    dataset = dataset[dataset["comp"].str.strip() == "Premier League"]

    # Strip the round variable to only keep the number
    dataset["round"] = dataset["round"].str.split().str[1]

    # Replace empty space with missing values
    dataset = dataset.replace(r"^\s*$", np.nan, regex=True)

    # Drop rows where there is no match data
    dataset = dataset.dropna(subset=["result"])

    # Create a list for object cols
    object_cols = ["date", "dayofweek", "round", "venue", "result", "opponent"]

    # Convert round to int64
    dataset["round"] = dataset["round"].astype("int64", errors="ignore")

    # # Except objectcols all columns should be numerical
    for col in dataset.columns:
        if col not in object_cols:
            dataset[col] = dataset[col].astype(float, errors="ignore")

    return dataset


def save_team_dataset(dataset, hostname, database, username, password, port):

    engine = create_engine(
        f"postgresql://{username}:{password}@{hostname}:{port}/{database}"
    )

    dataset.to_sql(
        "summary_team", engine, schema="public", if_exists="replace", index=False
    )
