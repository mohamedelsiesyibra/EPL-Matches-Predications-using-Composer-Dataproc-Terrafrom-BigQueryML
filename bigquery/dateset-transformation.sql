# Restructure the input table
CREATE OR REPLACE TABLE `premier_league_data.epl_matches_transformed` AS
(SELECT
  homeTeam as team,
  awayTeam as opponent,
  matchday,
  CAST(TIMESTAMP(date) AS DATE) as date,
  CASE
    WHEN winner = 'HOME_TEAM' THEN 1
    ELSE 0
  END AS win
FROM
  `premier_league_data.epl_matches_historical`)

UNION ALL

(SELECT
  awayTeam as team,
  homeTeam as opponent,
  matchday,
  CAST(TIMESTAMP(date) AS DATE) as date,
  CASE
    WHEN winner = 'AWAY_TEAM' THEN 1
    ELSE 0
  END AS win
FROM
  `premier_league_data.epl_matches_historical`);

-------------------------------------------------------------------------------------------------------------------------------

# Create the training data
CREATE OR REPLACE TABLE `premier_league_data.epl_train_data` AS
SELECT
  team,
  opponent,
  matchday,
  date,
  win
FROM
  `premier_league_data.epl_matches_transformed`
WHERE
  date < '2023-01-01';

-------------------------------------------------------------------------------------------------------------------------------

# Create the testing data
CREATE OR REPLACE TABLE `premier_league_data.epl_test_data` AS
SELECT
  team,
  opponent,
  matchday,
  date,
  win
FROM
  `premier_league_data.epl_matches_transformed`
WHERE
  date >= '2023-01-01';

-------------------------------------------------------------------------------------------------------------------------------

# Restructure the prediction table
CREATE OR REPLACE TABLE `premier_league_data.epl_matches_to_predict_transformed` AS
(SELECT
  homeTeam as team,
  awayTeam as opponent,
  matchday,
  CAST(TIMESTAMP(date) AS DATE) as date,
  NULL AS win
FROM
  `premier_league_data.epl_matches_next_season`)

UNION ALL

(SELECT
  awayTeam as team,
  homeTeam as opponent,
  matchday,
  CAST(TIMESTAMP(date) AS DATE) as date,
  NULL AS win
FROM
  `premier_league_data.epl_matches_next_season`);


