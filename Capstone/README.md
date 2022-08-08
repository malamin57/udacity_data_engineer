Summary  
This project will outline the process to build a data model to be used to populate a business intelligence (BI) tool. There was a request from the analytic team to create a data model for their team to help understand the immigration data for the United States. The analytics team will like to visualize data on how many people are coming from a certain location. In addition, the city they are traveling to while in the United States. To provide an insight of the data. Lastly, to create a table that might be relevant to the data to create further insights. 
To obtain this data, the data engineering team will gather data from the US National Tourism and Trade Office. On a high level this data provides data where the person is coming from and where they plan to visit. There is additional information about the individual such as visa type, visa type, and purpose to name a few columns in the data. Furthermore, the data engineering team will use additional data to help give more context about the immigration data. The additional that will be used will be the following:
1.World Temperature Data provide from Kaggle
2.U.S. City Demographic Data provide from OpenSoft
3.Airport Code table provide from Datahub
4.Reference Data for the Immigration data.
Furthermore, the data engineering team will leverage Spark and python to create a data model and pipelines to supply the data to the analytic team. The reason Spark will be used is because Spark is an open source program, to allow large datasets to process more easily.  Spark is known for its speed to handle large data sets and ease of use. The data will be scheduled to refresh once a month since the data from the US National Tourism and Trade Office is refreshed once a month. 
In addition, the data engineer wants to provide some insight on a few items that were asked in the meeting to clarify. In case of the data increase by 100% while working, Spark has the ability to parallelize the data. Which means the data will be split into additional working nodes to help compute the data. There are a few methods within Spark that can be used to perform this task, such as native libraries, thread pools or Pandas UDFs. Another question asked was how data needs to populate the dashboard on a daily basis by 7 am everyday. Since Spark doesn’t have a schedule or automated process, Spark can leverage Apache Airflow to help automate the refresh of the data in the dashboard. Airflow is an open-source workflow tool with ability to author, schedule, orchestrate, and monitor data pipelines. Furthermore, Airflow has a connection that connects to Spark. Lastly, if the data is accessed by 100 plus people, the data can be placed into an AWS Redshift database. 
Data Dictionary
World Temperature Data
dt - date 
AverageTemperature - the average temperature 
AverageTemperatureUncertainty - the 95% confidence interval around the average
City - The city the temperature was recorded 
Country - The country the temperature was recorded 
Latitude - Latitude of the country 
Longitude - Longitude of the country
U.S. City Demographic Data
City 
State
Median Age 
Male Population 
Female Population 
Total Population
Number of Veterans
Foreign-born
Average Household Size
State Code
Race - Black, Whites, and Hispanic or Latino
Count - This count the population of Black, Whites, and Hispanic or Latino 


Airport Code
Ident - identity code for a city 
Type - type of plane or helicopter used 
Name - Name of the airport 
Elevation_ft - elevation the airport 
Continent - continent of the airport 
Iso_country - country of the airport
Iso_region - region of the airport
Municipality - city of the airport 
Gps_code - GPS code of the airport
Iata_code - Iata code fo the airport 
Local_code - local code of the airport 
Coordinates - Coordinates of the airport 
Immigration Data 
Id - id of the person 
Cicid - custom id 
I94yr - year 
I94mon - month
I94cit - city origin
I94res - city origin
I94port - city destination 
Arrdate - arrival date 
I94mode - Transportation type air, sea, or land
I94addr - state destination 
Depdate - depart date 
I94bir - Age of Respondent in Years
I94visa - Visa code business, pleasure, student
Count - Used for summary statistics
Dtadfile - Character Date Field
Visapost -  Department of State where where Visa was issued 
Occup - Occupation that will be performed in U.S
Entdepa - Arrival Flag - admitted or paroled into the U.S.
Entdepd - Departure Flag - Departed, lost I-94 or is deceased
Entdepu - Update Flag - Either apprehended, overstayed, adjusted to perm residence
Matflag - Match flag - Match of arrival and departure records
Biryear - 4 digit year of birth
Dtaddto -  Character Date Field - Date to which admitted to U.S.
Gender - Male/Female 
Insnum - INS number
Airline - Airline used to arrive in the U.S.
Admnum - Admission Number
Fltno - Flight number of airlines used to arrive in the U.S.
Visatype - Class of admission legally admitting the non-immigrant to temporarily stay in the U.S. 

The schema that is used in this project will be the star schema. The star schema is widely used for various data projects. For this case since it is for a business intelligence project. This approach will suffice. The star schema allows the end user or tool to query data a lot faster since some data can be very large and return only data that is required instead of the entire table. Furthermore, the star consists of a model with tables called facts and dimensions. Dimension tables describe the business entities, such products people. In this case the immigration table will be the dimension table. The fact tables are observations or events of the data. Below are the fact tables used and an output of the data

1.Number of immigrants from a certain location

| I94CITValue |             arrdate | i94visa | visatype | count |
|-------------|---------------------|---------|----------|-------|
|             |                     |         |          |       |
|     croatia | 4/1/16 0:00         |       3 |       F1 |     1 |
|    slovenia | 4/1/16 0:00         |       1 |       WB |    14 |
|  china, prc | 4/1/16 0:00         |       1 |       B1 |   913 |
|     eritrea | 4/1/16 0:00         |       1 |       B1 |     2 |
|  argentina  | 4/1/16 0:00         |       1 |       B1 |   115 |

2.Number of immigrants to a certain location in the US

|       i94city |             arrdate |             depdate | Median Age | Male Population | Female Population | Total Population | Foreign-born |                 Race |   Count | count |
| ------------- | ------------------- | ------------------- | ---------- | --------------- | ----------------- | ---------------- | ------------ | -------------------- | ------- | ----- |
|               |                     |                     |            |                 |                   |                  |              |                      |         |       |
|       atlanta | 4/1/16 0:00         | ##########          | 33.8       | 223960          | 239915            | 463875           | 32016        | American Indian a... | 4606    | 35    |
|         miami | 4/1/16 0:00         | ##########          | 40.4       | 215840          | 225149            | 440989           | 260789       |                Asian | 4613    | 409   |
| san francisco | 4/1/16 0:00         | ##########          | 38.3       | 439752          | 425064            | 864816           | 297199       | American Indian a... | 8997    | 251   |
|        denver | 4/1/16 0:00         | 4/7/16 0:00         | 34.1       | 341137          | 341408            | 682545           | 113222       | American Indian a... | 14008   | 40    |
|      portland | 4/1/16 0:00         | 4/8/16 0:00         | 40.3       | 31480           | 35392             | 66872            | 9229         |   Hispanic or Latino | 2031    | 3     |

3.The average temperature of the origin 

| I94CITValue |             arrdate | avg(AverageTemperature) |
|-------------|---------------------|-------------------------|
|             |                     |                         |
|   guatemala | 4/1/16 0:00         |        18.6855693868142 |
|   guatemala | 4/2/16 0:00         |        18.6855693868142 |
|   guatemala | 4/3/16 0:00         |        18.6855693868142 |
|   guatemala | 4/4/16 0:00         |        18.6855693868142 |
|   guatemala | 4/5/16 0:00         |        18.6855693868142 |


4.The average temperature of the destination

| i94city      |             arrdate | avg(AverageTemperature) |
|--------------|---------------------|-------------------------|
|              |                     |                         |
|      atlanta | 1/1/00 0:00         |       13.94677781340173 |
| jacksonville | 1/1/00 0:00         |      20.545046489259377 |
|   gloucester | 1/1/00 0:00         |       8.517687934301959 |
|      chicago | 1/1/00 0:00         |       9.704552690226043 |
|    brunswick | 1/1/00 0:00         |       8.043903979785219 |

ETL Evidance 
Looking at the the data for individual who went to a certain you can’t tell if the correlation from the number of Foregin born and number of individual who visited that city. Since this data is taken from a small subset of the data. Hopefully, the analytic team can provide more insight into this data in their dashboard. 


|       i94city |             arrdate |             depdate | Median Age | Male Population | Female Population | Total Population | Foreign-born |                 Race |   Count | count |
| ------------- | ------------------- | ------------------- | ---------- | --------------- | ----------------- | ---------------- | ------------ | -------------------- | ------- | ----- |
|         miami | 4/1/16 0:00         | 4/17/16 0:00        | 40.4       | 215840          | 225149            | 440989           | 260789       |                Asian | 4613    | 409   |
| san francisco | 4/1/16 0:00         | 4/11/16 0:00        | 38.3       | 439752          | 425064            | 864816           | 297199       | American Indian a... | 8997    | 251   |
|       orlando | 4/1/16 0:00         | 4/12/16 0:00        | 33.1       | 130940          | 139977            | 270917           | 50558        |   Hispanic or Latino | 89306   | 193   |
|       seattle | 4/1/16 0:00         | 1/1/60 0:00         | 35.5       | 345659          | 338784            | 684443           | 119840       |                White | 511401  | 74    |
|     charlotte | 4/1/16 0:00         | 4/8/16 0:00         | 34.3       | 396646          | 430475            | 827121           | 128897       | American Indian a... | 8746    | 46    |
|        denver | 4/1/16 0:00         | 4/7/16 0:00         | 34.1       | 341137          | 341408            | 682545           | 113222       | American Indian a... | 14008   | 40    |
|       orlando | 4/1/16 0:00         | 4/24/16 0:00        | 33.1       | 130940          | 139977            | 270917           | 50558        | American Indian a... | 2374    | 37    |
|       atlanta | 4/1/16 0:00         | 4/23/16 0:00        | 33.8       | 223960          | 239915            | 463875           | 32016        | American Indian a... | 4606    | 35    |
|   los angeles | 4/1/16 0:00         | 4/26/16 0:00        | 35         | 1958998         | 2012898           | 3971896          | 1485425      |                White | 2177650 | 30    |
|        dallas | 4/1/16 0:00         | 4/25/16 0:00        | 32.6       | 639019          | 661063            | 1300082          | 326825       | American Indian a... | 17510   | 16    |
|   los angeles | 4/1/16 0:00         | 6/12/16 0:00        | 35         | 1958998         | 2012898           | 3971896          | 1485425      |   Hispanic or Latino | 1936732 | 11    |
|       houston | 4/1/16 0:00         | 6/28/16 0:00        | 32.6       | 1149686         | 1148942           | 2298628          | 696210       | Black or African-... | 529431  | 6     |
|  philadelphia | 4/1/16 0:00         | 4/26/16 0:00        | 34.1       | 741270          | 826172            | 1567442          | 205339       | Black or African-... | 691186  | 5     |
| san francisco | 4/1/16 0:00         | 5/23/16 0:00        | 38.3       | 439752          | 425064            | 864816           | 297199       |   Hispanic or Latino | 132114  | 4     |
|      portland | 4/1/16 0:00         | 4/8/16 0:00         | 40.3       | 31480           | 35392             | 66872            | 9229         |   Hispanic or Latino | 2031    | 3     |
|         tampa | 4/1/16 0:00         | 6/28/16 0:00        | 35.3       | 175517          | 193511            | 369028           | 58795        |                Asian | 15814   | 3     |
|     baltimore | 4/1/16 0:00         | 4/3/16 0:00         | 34.7       | 294027          | 327822            | 621849           | 49857        |   Hispanic or Latino | 29953   | 3     |
|  philadelphia | 4/1/16 0:00         | 6/28/16 0:00        | 34.1       | 741270          | 826172            | 1567442          | 205339       | Black or African-... | 691186  | 3     |
|       chicago | 4/1/16 0:00         | 7/7/16 0:00         | 34.2       | 1320015         | 1400541           | 2720556          | 573463       |                Asian | 195084  | 2     |
|       atlanta | 4/1/16 0:00         | 8/14/16 0:00        | 33.8       | 223960          | 239915            | 463875           | 32016        |   Hispanic or Latino | 18653   | 1     |


Source 
https://towardsdatascience.com/3-methods-for-parallelization-in-spark-6a1a4333b473
https://medium.com/codex/executing-spark-jobs-with-apache-airflow-3596717bbbe3
https://github.com/nadirl00/Data-Engineering-Capstone-Project/blob/master/I94_SAS_Labels_Descriptions.SAS
https://www.1week4.com/it/machine-learning/udacity-data-engineering-capstone-project/


 
 
 
 
 
 
 
 




eee





















































