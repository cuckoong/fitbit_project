* State table
state_code: string, state code, primary key, not null
state:string, state name, not null

* City table
city_id: long, city id, primary key not null
city: string, city name, not null

* Race table
race_id: long, race id, , primary key not null
race: string, race name, not null

* Race population table
population_id: long, population id, primary key not null
state_code: string, state code, foreign key, not null
city_id: long, city id, foreign key, not null
race_id: long, race id, foreign key, not null
population: long, population, not null
