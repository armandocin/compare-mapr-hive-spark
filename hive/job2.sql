drop table if exists input;
drop table if exists reviews;
drop table if exists avg_scores_per_year;

--row format delimited fields terminated by ${pattern};
CREATE TABLE input (line STRING); 

LOAD DATA LOCAL INPATH '/home/armandocin/hadoop-docker-volume/data/Reviews_ridotto.csv' OVERWRITE INTO TABLE input;

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

CREATE TABLE avg_scores_per_year AS
SELECT ProductId, collect_set(concat(reducer.Year, "=", cast(reducer.AvgScore as string))) as AvgPerYear
FROM(
	SELECT ProductId, Year, round(AVG(Score), 2) as AvgScore
	FROM(	
		SELECT ProductId, year(from_unixtime(Time)) as Year, Score
		FROM reviews
		) mapper
	WHERE Year > 2002
	GROUP BY ProductId, Year
	ORDER BY ProductId, Year
	) reducer
GROUP BY ProductId;
