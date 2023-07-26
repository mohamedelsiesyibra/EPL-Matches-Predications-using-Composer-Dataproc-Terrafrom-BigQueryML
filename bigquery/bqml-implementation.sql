# Create the model
CREATE OR REPLACE MODEL `ML_epl.epl_binary_logistic_reg_model`
OPTIONS(
  model_type='logistic_reg',
  auto_class_weights=TRUE,
  input_label_cols=['win']
) AS
SELECT
  win,
  matchday,
  team,
  opponent
FROM
  `premier_league_data.epl_train_data`;

---------------------------------------------------------------------------------------------------------------------------------------------

# Create the evaluation table 
CREATE OR REPLACE TABLE `premier_league_data.epl_model_evaluation` AS
SELECT
  *
FROM ML.EVALUATE(MODEL `ML_epl.epl_binary_logistic_reg_model`, 
   (SELECT
      win,
      matchday,
      team,
      opponent
     FROM
      `premier_league_data.epl_test_data`));

---------------------------------------------------------------------------------------------------------------------------------------------

# Create the prediction table 
CREATE OR REPLACE TABLE `premier_league_data.epl_matches_prediction` AS
SELECT
  team,
  opponent,
  matchday,
  predicted_win_probs[OFFSET(1)].prob as win_probability
FROM
  ML.PREDICT(MODEL `ML_epl.epl_binary_logistic_reg_model`,
    (SELECT
      team,
      opponent,
      matchday
     FROM
      `premier_league_data.epl_matches_to_predict_transformed`));

