# Big Data 2018 - First Project
*First project of Big Data course at Roma Tre University.*
a.y. 2017-2018

## Summary

- [Specifications](#specifications)
	+ [Goal](#goal)
	+ [Report](#report)

## Specifications

The dataset **Amazon Fine Food Reviews** ([Reviews.cvs.zip](http://torlone.dia.uniroma3.it/bigdata/Reviews.csv.zip)) is given. It contains about 600k reviews of gastronomic products released on Amazon from 1999 until 2012, and it is in csv format. Each line has the following fields:
- Id
- ProductId (unique identifier for the product)
- UserId (unique identifier for the user)
- Profile Name
- HelpfulnessNumerator (number of users who found the review helpful)
- HelpfulnessDenominator (number of users who graded the review)
- Score (rating between 1 and 5)
- Time (timestamp of the review expressed in [Unix Time](https://en.wikipedia.org/wiki/Unix_time))
- Summary (summary of the review)
- Text (text of the review)

### Goal

Project and implement in **MapReduce**, **Hive** and **Spark** the following jobs:
1. For each year, list the ten most used words in the field *Summary* of all reviews, ordered by frequency, specifing the number of occurrences of each word. Example:  

		1999	[word1_1=230, word2_1=207, ..., word10_1=70]
		2000	[word1_2=130, word2_2=111, ..., word10_2=34]
		...
		2012	[word1_14=200, word2_14=180, ..., word10_14=80]

2. For each product, list the average scores gained from 2003 and 2012, specifing the *ProductId* followed by all the average scores obtained during the years considered. The output should be ordered by ProductId. Example:  

		Prod_0	[2003=4.2, 2004=4.4, ..., 2012=4.8]
		Prod_1	[2003=3.1, 2004=3.0, ..., 2012=3.6]
		...

3. Pair of products having at least one user in common, that is, products that have been reviewed by the same user. For each pair must be specified the number of common users. The result should be ordered by the *ProductId* of the first element forming the pair, and possibly should not have duplicates. Example:  

		(Prod_0, Prod_1)	3
		(Prod_0, Prod_3)	1
		...
		(Prod_1, Prod_0) ---> duplicate
		(Prod_1, Prod_2)	4
		...

### Report

A final report must be written for each job. The report should contain:

- A possible MapReduce, Hive, Spark **implementation** (pseudocode).
- The beginning lines of each job result.
- Tables and plots comparing local and cluster execution time of each job.
