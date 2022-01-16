# Helpers
import helpers.player_dataset as player_functions
import helpers.squad_dataset as squad_functions
import helpers.team_dataset as team_functions

# DB configuration
import os
from dotenv import load_dotenv
from pathlib import Path

import time

# Config parameters
links = [
    "https://fbref.com/en/squads/18bb7c10/Arsenal-Stats",
    "https://fbref.com/en/squads/cff3d9bb/Chelsea-Stats",
    "https://fbref.com/en/squads/822bd0ba/Liverpool-Stats",
    "https://fbref.com/en/squads/b8fd03ef/Manchester-City-Stats",
    "https://fbref.com/en/squads/7c21e445/West-Ham-United-Stats",
    "https://fbref.com/en/squads/19538871/Manchester-United-Stats",
    "https://fbref.com/en/squads/361ca564/Tottenham-Hotspur-Stats",
    "https://fbref.com/en/squads/a2d435b3/Leicester-City-Stats",
]

team_names = [
    "Arsenal",
    "Chelsea",
    "Liverpool",
    "Manchester City",
    "West Ham",
    "Manchester United",
    "Tottenham",
    "Leicester",
]

team_table_id = "matchlogs_for"
table_ids = [
    "stats_standard_11160",
    "stats_keeper_adv_11160",
    "stats_shooting_11160",
    "stats_passing_11160",
    "stats_gca_11160",
    "stats_defense_11160",
    "stats_possession_11160",
]

data_types = [
    "summary",
    "keeping",
    "shooting",
    "passing",
    "goal_creation",
    "defense",
    "possession",
]

# Main scraping function
def scrape_fbref_data(
    links,
    team_names,
    team_table_id,
    table_ids,
    data_types,
    hostname,
    database,
    username,
    password,
    port,
):

    print(f"Scraping team summary")
    team_functions.save_team_dataset(
        team_functions.clean_team_dataset(
            team_functions.create_team_dataset(
                table_id=team_table_id, links=links, team_names=team_names
            )
        ),
        hostname=hostname,
        database=database,
        username=username,
        password=password,
        port=port,
    )

    for table_id, data_type in zip(table_ids, data_types):

        print(f"Scraping squad {data_type}")
        squad_functions.save_squad_dataset(
            squad_functions.clean_squad_dataset(
                squad_functions.create_squad_dataset(
                    table_id=table_id, links=links, team_names=team_names
                )
            ),
            data_type=data_type,
            hostname=hostname,
            database=database,
            username=username,
            password=password,
            port=port,
        )

        print(f"Scraping player {data_type}")
        player_functions.save_player_dataset(
            player_functions.clean_player_dataset(
                player_functions.create_player_dataset(table_id, links, team_names)
            ),
            data_type=data_type,
            hostname=hostname,
            database=database,
            username=username,
            password=password,
            port=port,
        )

    print("Scraping complete")


if __name__ == "__main__":

    path = os.path.join(Path(__file__).parent, "config/db_secrets.env")

    load_dotenv(dotenv_path=path)

    params = {}
    params["links"] = links
    params["team_names"] = team_names
    params["team_table_id"] = team_table_id
    params["table_ids"] = table_ids
    params["data_types"] = data_types
    params["hostname"] = os.getenv("HOSTNAME")
    params["database"] = os.getenv("DATABASE")
    params["username"] = os.getenv("USERNAME")
    params["password"] = os.getenv("PASSWORD")
    params["port"] = os.getenv("PORT")

    start = time.time()

    scrape_fbref_data(**params)

    end = time.time()
    print(f"Runtime of scraping function is {(end-start)/60} minutes")
