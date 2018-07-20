/* Nicolas Eldering
 * CSC 643 -- Big Data
 * 3/19/2018
 * Assignment 2 
 * Problem 3
 */

---------------------------------------Start of rainQ3.pig -----------------------
line = LOAD'hdfs://sophia:9000/user/hduser/input/3240jul2013.dat' USING PigStorage() AS (
    total:chararray
); -- bring in data from the hdfs

stateID_precipitation = FOREACH line GENERATE SUBSTRING(total, 3, 5) as stateID, SUBSTRING(total,23, 27 ) as day, flatten(STRSPLIT(total, ' 2500 ', 2)) as (noMatter, precipitation); -- grab the state id, day, and total rainfall

stateID_precipitation_rtrim = FOREACH stateID_precipitation GENERATE stateID, (int)day, RTRIM(precipitation) as precipitation; -- clean our precipitation data

stateID_precipitation_withoutI = FILTER stateID_precipitation_rtrim by not(precipitation matches '.*I\\b'); -- remove unneeded I records

july_4th_days = FILTER stateID_precipitation_withoutI by day == 4; -- filter only july 4th

stateID_rainfall = FOREACH july_4th_days GENERATE $0, (int)(SUBSTRING($2, 0, 5)) as rainfall; -- allow us to group by state

julyState = GROUP stateID_rainfall by stateID; -- group our july 4th data by stateid

julyClean = FOREACH julyState GENERATE group as stateID,  SUM(stateID_rainfall.rainfall); -- get the total for each states july 4th

results = FILTER julyClean by $1 >= 100; -- filter for states that got more than an inch of rain

dump results; -- show our results


--------------------------------------End of Script ----------------------------

