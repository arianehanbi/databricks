# databricks
Apache Spark SQL

<br>
<br>

# Create Table
Create table using the Parquet file format, Parquet is an open-source, column-based file format. You can specify how you want your table to be written with the USING keyword.
```
CREATE TABLE People10M
USING parquet OPTIONS (
  path "/mnt/training/dataframes/people-10m.parquet",
  header "true"
);
```

```
DROP TABLE IF EXISTS movieRatings;
CREATE TABLE movieRatings (
  userId INT,
  movieId INT,
  rating FLOAT,
  timeRecorded INT
) USING csv OPTIONS (
  PATH "/mnt/training/movies/20m/ratings.csv",
  header "true"
);
```

<br>
<br>

# Temporary Views
Temporary views are useful for data exploration. It gives you a name to query from SQL, but unlike a table, does not carry over when you restart the cluster or switch to a new notebook.

```
CREATE OR REPLACE TEMPORARY VIEW PeopleSavings AS
SELECT
  firstName,
  lastName,
  year(birthDate) as birthYear,
  salary,
  salary * 0.2 AS savings
FROM
  People10M;
```

<br>
<br>

# Quarying Tables

* **Cast()**: show the timestamp as a human-readable time and date. ex) CAST(timeRecorded as timestamp) 


```
SELECT count(DISTINCT firstName)
FROM SSANames;
```

<br>
<br>

# Join Two Tables

1. Create temporary views: we create two temporary views so that the actual join will be easy to read/write.
2. Perform join

```
CREATE OR REPLACE TEMPORARY VIEW SSADistinctNames AS 
  SELECT DISTINCT firstName AS ssaFirstName 
  FROM SSANames;

CREATE OR REPLACE TEMPORARY VIEW PeopleDistinctNames AS 
  SELECT DISTINCT firstName 
  FROM People10M
  
SELECT firstName 
FROM PeopleDistinctNames 
JOIN SSADistinctNames ON firstName = ssaFirstName

```

<br>
<br>


#

```
```

<br>
<br>


<br>
<br>


<br>
<br>
