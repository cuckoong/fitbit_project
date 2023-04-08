## panas_table
* panas_id: long, panas id, primary key not null
* userId: string, user id, not null
* dateTime: timestamp, date and time, not null
* year: int, year, not null
* month: int, month, not null
* day: int, day, not null
* P1[SQ001] - P1[SQ020]: int, P1[SQ001] - P1[SQ020], not null

## personality_table
* personality_id: long, personality id, primary key not null
* userId: string, user id, not null
* extraversion: int, extraversion, not null
* agreeableness: int, agreeableness, not null
* conscientiousness: int, conscientiousness, not null
* stability: int, stability, not null
* intellect: int, intellect, not null

## facts_table
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
