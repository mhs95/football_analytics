# football_analytics

## Overview
This repository contains code to scrape, store and analyse all squad, player and player-match level statistics for the 2021-2022 English Premier League season from [FBREF](https://fbref.com/en/). 
Currently the scraped data is limited to the following teams:
  - Arsenal F.C.
  - Chelsea F.C. 
  - Leceister City F.C. 
  - Liverpool F.C. 
  - Manchester City F.C. 
  - Manchester United F.C. 
  - Tottenham Hotspur F.C. 
  - West Ham United F.C. 

## Structure 
1. src
  - scraping
    - main.py: This python script scrapes all relevant squad, player and player-match level data and stores it in a PostGres database. 
    - config
      - db_secrets.env: This file contains access credentials to the PostGres database. Please reach out to Hassan Saad (mhsaad1995@gmail.com) for access credentials to the database. 
    - helpers
      - player_dataset.py: This file contains helper functions to scrape player-match level data. 
      - squad_dataset.py: This file contains helper functions to scrape player level data.
      - team_dataset.py: This file contains helper functions to scrape team level data. 
    
2. playground
  - hassan: This folder contains jupyter notebooks to conduct exploratory data analysis of the scraped data
    
3. data
  - input/scraped: This folder consists of individual folders for each team which in turn contain all the different datasets created through the scraping process. Please note that the data contained in the folder is preliminary data scraped and saved locally before moving storage to the PostGres database. Therefore this data only serves as a reference to the types of data scraped and tables created in the PostGres database.
  - output/graphs: This folder contains graphs created in the EDA process. 
  
## Further improvements
Please see the list below for further additions in the pipeline:
  - Scrape data for all English Premier League teams
  - Build a publicly available analytics dashboard for all squad, player and player-match level statistics
  - Apply machine learning algorithms to build match-score prediction models
