/* Nicolas Eldering and Yifan Chen (Team 3)
 * CSC 643 -- Big Data
 * 3/19/2018
 * Assignment 2 
 * Problem 1
 */

-------------------------------------------Start of rainQ1 ----------------------------------
line = LOAD'hdfs://sophia:9000/user/hduser/input/3240sep2013.dat' USING PigStorage() AS (
    total:chararray
); -- bring in data from the hdfs

stateID_precipitation = FOREACH line GENERATE SUBSTRING(total, 3, 5) as stateID, flatten(STRSPLIT(total, ' 2500 ', 2)) as (noMatter, precipitation); -- grab the state id and total rain

stateID_precipitation_rtrim = FOREACH stateID_precipitation GENERATE stateID, RTRIM(precipitation) as precipitation; -- clean our precipitation data

stateID_precipitation_withoutI = FILTER stateID_precipitation_rtrim by not(precipitation matches '.*I\\b'); -- remove unneeded I records

stateID_rainfall = FOREACH stateID_precipitation_withoutI GENERATE $0, (int)(SUBSTRING($1, 0, 5)) as rainfall; -- allow us to group by state

septState = GROUP stateID_rainfall by stateID; -- group our data by state

septClean = FOREACH septState GENERATE group as stateID,  SUM(stateID_rainfall.rainfall)/30 as rainfall; 

septSort = order septClean by rainfall desc; -- sort so we can grab the max

max = LIMIT septSort 1; -- grab our max result
 
dump max; -- show our results
---------------------------------------End of Script --------------------------

