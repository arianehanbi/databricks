# databricks
Apache Spark SQL

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

# Quarying Tables

* **Cast()**: show the timestamp as a human-readable time and date. ex) CAST(timeRecorded as timestamp) 


```
SELECT count(DISTINCT firstName)
FROM SSANames;
```

* View metadata; Detailed Table Information contains information about the table's database name, original source file type and location, and more.

```
DESCRIBE tablename;
DESCRIBE EXTENDED tablename;
```

* Instead of simply returning the top rows, we can get a random sampling of rows using the function RAND() to return random rows.

```
SELECT * FROM tablename
ORDER BY RAND()
LIMIT 3;
```

* Explode a nested object: 

We can observe from the output that the column contains a nested object with named key-value pairs. <br>

**EXPLODE** is used with arrays and elements of a map expression. When used with an array, it splits the elements into multiple rows. Used with a map, it splits the elements of a map into multiple rows and columns and uses the default names, key and value, to name the new columns. 

```
SELECT EXPLODE(colname) FROM DCDataRaw;
```

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


# Caching Table

- Caching places a table into temporary storage across the cluster.
- However, if we are storing data in memory, there is some time and storage cost. So, be careful!

```
CACHE TABLE tablename;

UNCACHE TABLE IF EXISTS tablename;
```

<br>

#

```
```

<br>

#

```
```

<br>

#

```
```

<br>

