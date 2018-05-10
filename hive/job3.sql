drop table if exists input;
drop table if exists reviews;
drop table if exists common_users_per_prod_pair;

--row format delimited fields terminated by ${pattern};
CREATE TABLE input (line STRING); 

LOAD DATA LOCAL INPATH '/home/armandocin/hadoop-docker-volume/data/Reviews_ridotto50k.csv' OVERWRITE INTO TABLE input;

set hivevar:pattern = ",(?=([^\\\"]*\\\"[^\\\"]*\\\")*(?![^\\\"]*\\\"))";

CREATE TABLE reviews AS
SELECT split(line, ${pattern})[0] as Id,
	split(line, ${pattern})[1] as ProductId,
	split(line, ${pattern})[2] as UserId,
	split(line, ${pattern})[3] as ProfileName,
	cast( split(line, ${pattern})[4] as INT ) as HelpfulnessNumerator,
	cast( split(line, ${pattern})[5] as INT ) as HelpfulnessDenominator,
	cast( split(line, ${pattern})[6] as INT ) as Score,
	cast(split(line, ${pattern})[7] as BIGINT) as Time,
	split(line, ${pattern})[8] as Summary,
	split(line, ${pattern})[9] as Text
FROM input;

CREATE TABLE common_users_per_prod_pair AS
SELECT t1.ProductId as Product1, t2.ProductId as Product2, COUNT(1) as CommonUsersNum
FROM(
	SELECT DISTINCT ProductId, UserId
	FROM reviews
	ORDER BY ProductId
	) t1
	JOIN
	(
	SELECT DISTINCT ProductId, UserId
	FROM reviews
	ORDER BY ProductId
	) t2
	ON t1.UserId = t2.UserId
WHERE t1.ProductId < t2.ProductId --do not select duplicate pairs
GROUP BY t1.ProductId, t2.ProductId
HAVING t1.ProductId != t2.ProductId
ORDER BY t1.ProductId, t2.ProductId
