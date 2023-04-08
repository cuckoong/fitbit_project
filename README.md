# Project Title
Data Engineering Capstone Project

## Project Summary
The objective of this project is to perform data ETL on Fitbit user data. This involves extracting exercise-related 
information from Fitbit sensors, such as step counts, distance traveled, and calories burned, along with user-generated 
data about their emotional state and personality. By establishing a two-dimensional data model and one fact table, the 
resulting dataset will allow for in-depth analysis of the relationship between exercise and emotional wellbeing. 
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

## Steps to run the project
1. Clone the repository and navigate to the downloaded folder.
    * ` git clone `
    * ` cd capstone_project`
2. Create a virtual environment and install the required packages with requirements.txt.
    * `conda create -n fitbit_project python=3.10`
    * `conda activate fitbit_project`
    * `conda install requirements.txt`
    * `conda activate fitbit_project`
3. Install MongoDB Community Edition from [here](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-os-x/).
4. Set up the MongoDB server, add add the bson files to the database.
    * `cd data`
    * `mongorestore -d rais_anonymized -c fitbit fitbit.bson`
    * `mongorestore -d rais_anonymized -c surveys surveys.bson`
    * `cd ..`
5. start the MongoDB server by running the following command in the terminal:
    * `brew services start mongodb-community`
6. Run the following command to run the ETL pipeline that cleans and stores the data in the database.
    * `python etl.py`
7. Run the test file to check the etl pipeline.
    * `python test.py`
8. Run the quality check file to check the quality of the data.
    * `python quality_check.py`