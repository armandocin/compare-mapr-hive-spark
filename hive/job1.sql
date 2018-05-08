drop table if exists input;
drop table if exists reviews;

CREATE TABLE input (line STRING) ;

LOAD DATA LOCAL INPATH '/home/armandocin/hadoop-docker-volume/data/Reviews_ridotto.csv' OVERWRITE INTO TABLE input;

set hivevar:pattern = ",(?=([^\\\"]*\\\"[^\\\"]*\\\")*(?![^\\\"]*\\\"))";

CREATE TABLE reviews as
SELECT split(line, ${pattern})[0] as Id,
		split(line, ${pattern})[1] as ProductId,
		split(line, ${pattern})[2] as UserId,
		split(line, ${pattern})[3] as ProfileName,
		split(line, ${pattern})[4] as HelpfulnessNumerator,
		split(line, ${pattern})[5] as HelpfulnessDenominator,
		split(line, ${pattern})[6] as Score,
		split(line, ${pattern})[7] as Time,
		split(line, ${pattern})[8] as Summary,
		split(line, ${pattern})[9] as Text
FROM input;

--year(from_unixtime(cast(split(line, ${pattern})[7] as bigint)))-->

CREATE TABLE mapper as
