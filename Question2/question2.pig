/* Yifan Chen
 * CSC 643 -- Big Data
 * 3/19/2018
 * Assignment 2 
 * Problem 2
 */

line = LOAD'hdfs://sophia:9000/user/precipitation/3240{jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec}2013.dat' USING PigStorage() AS (
	total:chararray
);

stateID_precipitation = FOREACH line GENERATE SUBSTRING(total, 3, 5) as stateID, flatten(STRSPLIT(total, ' 2500 ', 2)) as (nomatter, precipitation);

stateID_precipitation_rtrim = FOREACH stateID_precipitation GENERATE stateID, RTRIM(precipitation) as precipitation;

stateID_precipitation_withoutI = FILTER stateID_precipitation_rtrim by not(precipitation matches '.*I\\b');

stateID_rainfall = FOREACH stateID_precipitation_withoutI GENERATE $0, (int)(SUBSTRING($1, 0, 5)) as rainfall;

stateID_rainfall_group = GROUP stateID_rainfall by stateID;

total_precipitation = FOREACH stateID_rainfall_group GENERATE group as stateID, SUM(stateID_rainfall.rainfall) as totalRainfall;

total_precipitation_ordered = ORDER total_precipitation BY totalRainfall;

total_precipitation_limited = LIMIT total_precipitation_ordered 1;

DUMP total_precipitation_limited;