# Anamoly-Detection

CREATE EXTERNAL TABLE sensorinput (
       highway int,		-- highway id
       sensorloc int,		-- one sensor location may have 
       -- multiple sensors, e.g. for different highway lanes
       sensorid int, 		-- sensor id
       dayofyear bigint,		-- yyyyddd 
       dayofweek bigint,		-- 0=Sunday, 1=Monday, etc 
       time decimal(10,2), 	-- seconds since midnight
				-- e.g. a value of 185.67 is 3:05:67 a.m.
       volume int, 		-- a count
       speed int, 			-- average, in m.p.h.
       occupancy int		-- a count
)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION 's3://aws-bigdata-blog/artifacts/anomaly-detection-using-pyspark/sensorinput/';

CREATE EXTERNAL TABLE kcalcs (
     run string, 
     wssse decimal(20,3),
     k decimal, 
     clusterid decimal, 
     clustersize decimal, 
     volcntr decimal(10,1),
     spdcntr decimal(10,1),
     occcntr decimal(10,1),
     volstddev decimal(10,1),
     spdstddev decimal(10,1),
     occstddev decimal(10,1)
)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION 's3://aws-bigdata-blog/artifacts/anomaly-detection-using-pyspark/sensorclusters/'
tblproperties ("skip.header.line.count"="2");

SELECT DISTINCT k, clusterid, clustersize, volcntr, spdcntr, occcntr, volstddev, spdstddev, occstddev
	FROM kcalcs
	ORDER BY k, spdcntr;
spark-submit /mnt/var/kmeansandey.py s3://aws-bigdata-blog/artifacts/anomaly-detection-using-pyspark/sensorinputsmall/ 4 s3://<your_bucket>/sensoroutputsmall/

SELECT sensorid, clusterid, concat(cast(sensorid AS string), '.', cast(clusterid AS string)) AS senclust, count(*) AS howmany, max(maldist) AS dist
FROM sensoroutput
GROUP BY sensorid, clusterid
ORDER BY sensorid, clusterid;

SELECT sensorid, clusterid, count(*) AS num_outliers, avg(spdcntr) AS spdcntr, avg(maldist) AS avgdist
FROM sensoroutput
WHERE  maldist > 2.5
GROUP BY sensorid, clusterid
ORDER BY sensorid, clusterid;
