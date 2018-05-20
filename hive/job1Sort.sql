drop table if exists input;
drop table if exists reviews;
drop table if exists result;
drop table if exists output;

add jar /home/armandocin/Documenti/bigdata_git/hive/lib/hiveUDF-0.0.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION limit_list AS 'utils.LimitCollectionLengthUDF';

--row format delimited fields terminated by ${pattern};
CREATE TABLE input (line STRING); 

LOAD DATA LOCAL INPATH '/home/armandocin/hadoop-docker-volume/data/input_proj1/Reviews.csv' OVERWRITE INTO TABLE input;

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

CREATE TABLE result AS
SELECT t2.Year, limit_list(collect_set(concat(t2.Word, "=", cast(t2.Count as string)))) as WordCounts
FROM
	(
	SELECT t1.Year, t1.Word, COUNT(1) as Count
	FROM(
		SELECT year(from_unixtime(Time)) as Year, lower(regexp_replace(Word, '[\\-\\+\\.\\^:,\"\'$%&(){}£=#@!?\t\n]', '')) as Word
		FROM reviews
		LATERAL VIEW explode( split(Summary, "\\s+") ) exp AS Word
		) t1
	WHERE t1.Year is not NULL
	GROUP BY t1.Year, t1.Word
	DISTRIBUTE BY t1.Year, t1.Word
	SORT BY t1.Year ASC, Count DESC
	) t2
WHERE t2.Word != ""
GROUP BY t2.Year;

create external table output (Year int, WordCounts array<string>)
row format delimited
fields terminated by '\t'
collection items terminated by ',\s'
lines terminated by '\n'
stored as textfile location '/user/hive/warehouse/output1';
insert into table output select * from result;
