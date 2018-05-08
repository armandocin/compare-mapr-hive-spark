drop table if exists input;
drop table if exists reviews;
drop table if exists mapper;
drop table if exists reducer;

CREATE TABLE input (line STRING) ;

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

CREATE TABLE mapper AS
SELECT t2.Year, concat(t2.Word, "=", cast(t2.Count as string)) as keyvalue
FROM
	(
	SELECT Year, Word, COUNT(1) as Count
	FROM(
		SELECT year(from_unixtime(Time)) as Year, lower(regexp_replace(Word, '[\\-\\+\\.\\^:,\"\'$%&(){}£=#@!?\t\n]', '')) as Word
		FROM reviews
		LATERAL VIEW explode( split(Summary, "\\s+") ) exp AS Word
		) t1
	WHERE t1.Year is not NULL
	GROUP BY Year, Word
	ORDER BY Year ASC, Count DESC
	) t2
WHERE t2.Word != "";

CREATE TABLE reducer AS
SELECT Year, collect_set(keyvalue) as WordsCount
FROM mapper
GROUP BY Year
ORDER BY Year;
