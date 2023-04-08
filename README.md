# Data Engineering Capstone Project

## Project Summary
The objective of this project is to perform data ETL on Fitbit user data. This involves extracting exercise-related 
information from Fitbit sensors, such as step counts, distance traveled, and calories burned, along with user-generated 
data about their emotional state and personality. By establishing a two-dimensional data model and one fact table, the 
resulting dataset will allow for in-depth analysis of the relationship between exercise and emotional health. 
Additionally, this project enables future exploration of how an exercise tracking system could be utilized to improve 
both physical and mental health outcomes for users.

## Scope of the Project
The "LifeSnaps" dataset is a multi-modal and longitudinal dataset containing over 35 different data types collected from 
71 participants over a period of more than 4 months. It includes data on physical activity, sleep, stress, and overall 
health, as well as behavioral and psychological measurements. The data was collected unobtrusively through surveys, 
real-time assessments, and a Fitbit Sense. LifeSnaps contains more than 35 different data types from second to daily 
granularity, totaling more than 71M rows of data. The dataset is intended to enable research in various fields,
including medical digital innovations, data privacy, well-being, psychology, machine learning, and human-computer interaction.

## Describe and Gather Data
* The "LifeSnaps" dataset is from [Link](https://zenodo.org/record/7229547#.ZB7CC-xByrM).
* The dataset used in this project:
    * fitbit sensor data
        * fitbit.bson
    * fitbit survey data
        * surveys.bson
    * fitbit user personality data
        * personality.csv

## ETL Pipeline
The ETL pipeline is as follows:
### Fitbit Sensor Data
1. Extract fitbit sensor data from the "LifeSnaps" dataset.
2. Select sensor data types of step counts, distance traveled, and calories burned.
3. Drop rows with missing values, and drop duplicates.
4. Extract year, month, day by the 'dataTime' column.
5. Set the columns to be not null.
6. Add the primary key "fitbit_id" to the dataframe.
7. Repartition the dataframe into 4 partitions, using the "userId" column as the partition key.
8. Save the dataframe as a parquet file as 'data/processed/fitbit_table.parquet'

### Fitbit Survey Data
1. Extract fitbit survey data from the "LifeSnaps" dataset.
2. Select survey data types with only the PANAS questions.
3. Drop rows with missing values, and drop duplicates.
4. Extract year, month, day by the 'dataTime' column.
5. Set the columns to be not null.
6. Add the primary key "panas_id" to the dataframe.
7. Repartition the dataframe into 4 partitions, using the "userId" column as the partition key.
8. Save the dataframe as a parquet file as 'data/processed/panas_table.parquet'

### Fitbit User Personality Data
1. Extract fitbit user personality data from the "LifeSnaps" dataset.
2. Select columns with only the Big Five personality questions.
3. Drop rows with missing values, and drop duplicates.
4. Add the primary key "personality_id" to the dataframe.
5. Repartition the dataframe into 4 partitions, using the "userId" column as the partition key.
6. Save the dataframe as a parquet file as 'data/processed/personality_table.parquet'

### Facts Table
1. Read the parquet files from the previous steps.
2. Join the panas_table and personality_table on the "userId" column, and select the sensor data from the fitbit_table 
within the 7 days before the survey date.
3. Calculate the mean and standard deviation of the sensor data (mean_steps, mean_distance, sd_steps, sd_distance,
mean_calories, sd_calories), and save them as columns. Also add the 'panas_id' column from the 'panas_table.
4. Join the fact_table with the personality_table on the "userId" column, add 'personality_id' column.
5. Also add the 'year', 'month', 'day' columns from the 'panas_table.
6. Add the primary key "id" to the dataframe.
7. Repartition the dataframe into 4 partitions, using the "userId" column as the partition key.
8. Save the dataframe as a parquet file as 'data/processed/facts_table.parquet'


## Data Model (Star Schema)
The data model is a star schema with one fact table and two dimension tables. 

* Dimension Tables:
The dimension tables are "personality_table" and "panas_table", which contain user information and user-generated data 
about their emotional state and personality, respectively.
    * dim_panas_table, with panas_id as primary key, contains information about the users' panas level in each submitted date.
        * columns: panas_id, userId, dateTime, year, month, day, panas_score (P1[SQ001]-P1[SQ020])
    * dim_personality_table, with personality_id as primary key, contains information about the user's personality.
        * columns: personality_id, userId, extraversion, agreeableness, conscientiousness, stability, intellect.
    
* Fact Table:
The fact table is the "facts" table, which contains the exercise-related information from Fitbit sensors, such as step
counts, distance traveled, and calories burned, which are the mean and sd values of one-week intervals before the survey date.
    * fact_table, with id as primary key, contains information about the users' fitbit data, panas level, and personality.
        * columns: id, panas_id, personality_id, userId, year, month, day, mean_steps, mean_distance, sd_steps, sd_distance,
      mean_calories, sd_calories.


## Data Dictionary
### panas_table
* panas_id: long, panas id, primary key not null
* userId: string, user id, not null
* dateTime: timestamp, date and time, not null
* year: int, year, not null
* month: int, month, not null
* day: int, day, not null
* P1[SQ001] - P1[SQ020]: int, P1[SQ001] - P1[SQ020], not null

### personality_table
* personality_id: long, personality id, primary key not null
* userId: string, user id, not null
* extraversion: int, extraversion, not null
* agreeableness: int, agreeableness, not null
* conscientiousness: int, conscientiousness, not null
* stability: int, stability, not null
* intellect: int, intellect, not null

### facts_table
* id: long, id, primary key not null
* panas_id: long, panas id, foreign key not null
* personality_id: long, personality id, foreign key not null
* userId: string, user id, not null
* year: int, year, not null
* month: int, month, not null
* day: int, day, not null
* mean_steps: double, mean steps of one-week interval before survey date, not null
* sd_steps: double, standard deviation of steps of one-week interval before survey date, not null
* mean_distance: double, mean distance of one-week interval before survey date, not null
* sd_distance: double, standard deviation of distance of one-week interval before survey date, not null
* mean_calories: double, mean calories of one-week interval before survey date, not null
* sd_calories: double, standard deviation of calories of one-week interval before survey date, not null



## Steps to run the project
1. Clone the repository and navigate to the downloaded folder.
    * `git clone https://github.com/cuckoong/fitbit_project.git`
    * `cd fitbit_project`
2. Create a virtual environment and install the required packages with requirements.txt.
    * `conda env create -f environment.yml`
    * `conda activate fitbit_project`

3. Install MongoDB Community Edition from [here](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-os-x/).
4. Set up the MongoDB server, add the bson files to the database.
    * `cd data`
    * `mongorestore -d rais_anonymized -c fitbit fitbit.bson`
    * `mongorestore -d rais_anonymized -c surveys surveys.bson`
    * `cd ..`
5. start the MongoDB server by running the following command in the terminal:
    * `brew services start mongodb-community`
6. Run the following command to run the ETL pipeline that cleans and stores the data in the database.
    * `python src/etl.py`
7. Run the test file to check the etl pipeline.
    * `pytest test/test_etl.py`
8. Run the quality check file to check the quality of the data.
    * `pytest test/test_quality_check.py`


## Complete Project Write Up
1. Clearly state the rationale for the choice of tools and technologies for the project.
    * Apache Spark is used to do all the data processing and data model creation. Since Spark is able to handle data 
  transformation, even when the data is scaled up to lots of terabytes.

2. Propose how often the data should be updated and why.
    * The data should be updated every week, since the data is collected from Fitbit sensors, which is a wearable device.
    
3. Write a description of how you would approach the problem differently under the following scenarios: 
   1) The data was increased by 100x.
        * Spark could handle data scale up by 100x, we just need to design the partition ways to help improve
      the processing efficiency, and the computation resources we may use for data update.
     
   2) The data populates a dashboard that must be updated on a daily basis by 7am every day.
       * We can set up the Apache Airflow for dashboard update. A schedule could be used to trigger the data processing with 
      desired update frequency, and an SLA (service level agreement) could be used to define when the tasks must complete. 
      In addition, Airflow could send emails when the tasks successes, failures, or retries.
   3) The database needed to be accessed by 100+ people.
        * Add the database storage to cloud services like S3 storage in Amazon Web services. People will be authorized to
      access to the database in the S3 bucket. Also add the database uploading task to the data pipline in Apache AirFlow 
      to ensure the database on the cloud is updated.
