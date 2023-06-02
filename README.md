# databricks
Apache Spark SQL

<br>

# Create Table
- Create table using the Parquet file format.
**Parquet** is an open-source, column-based file format. You can specify how you want your table to be written with the USING keyword.
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

```
SELECT count(DISTINCT firstName)
FROM tablename;
```

#### DESCRIBE 
View metadata; Detailed Table Information contains information about the table's database name, original source file type and location, and more. With **EXTENDED**, we will get more detailed table information, like which database holds the data and the table name.
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

* Sample the table

While accessing a random sample using the **RAND()** is a common way to retrieve a sample with other SQL dialects, Spark SQL includes a built-in function that you may want to use instead. The function, **TABLESAMPLE**, allows you to return a number of rows or a certain percentage of the data. In the cell directly below this one, we show that TABLESAMPLE can be used to access a specific number of rows. In the following, we show that it can be used to access a given percentage of the data. 

```
SELECT * FROM tablename TABLESAMPLE (5 ROWS)
SELECT * FROM tablename TABLESAMPLE (2 PERCENT) ORDER BY colname 
```

* Explode a nested object: We can observe from the output that the column contains a nested object with named key-value pairs. <br>

```
SELECT EXPLODE(colname) FROM tablename;
```

**EXPLODE** is used with arrays and elements of a map expression. When used with an array, it splits the elements into multiple rows. Used with a map, it splits the elements of a map into multiple rows and columns and uses the default names, key and value, to name the new columns. 


* Checking NULLs

```
SELECT count(*) FROM tablename WHERE Description IS NULL;
```

* COALESCE()

We can use it to **replace NULL values**. For all NULL values in the Description column, COALESCE() will replace the null with a value you include in the function.

```
COALESCE(colname, 0)
```

* SPLIT()
This command **splits a string value around a specified character** and returns an array. An array is a list of values that you can access by position. This list is zero-indexed for the index of the first position is 0. ex. Date 1/10/21 8:26

```
SPLIT(Date, "/")[0] month
SPLIT(Date, "/")[1] day
SPLIT(SPLIT(Date, " ")[0], "/")[2] year
```

* LPAD()
It **inserts characters to the left of a string** until the string reachers a certain length. In this example, we use LPAD to insert a zero to the left of any value in the month or day column that is not two digits. 

```
LPAD(month, 2, 0) AS month
LPAD(day, 2, 0) AS day
```

* CONCAT_WS()
returns a concatenated string with a specified separator.

```
CONCAT_WS("/", month, day, year) sDate
TO_DATE(sDate, "MM/dd/yy") date
```

* CAST()
**Cast()** show the timestamp as a human-readable time and date.

```
CAST(timeRecorded as timestamp) 
CAST(UnitPrice AS DOUBLE)
```

*

```
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

# Common Table Expressions (CTE) 

CTEs are supported in Spark SQL \[**CTE starts with a WITH clause**\]. A CTE provides a temporary result set which you can then use in a SELECT statement. These are different from **temporary views** in that they cannot be used beyond the scope of a single query. In this case, we will use the CTE to get a closer look at the nested data without writing a new table or view. CTEs use the WITH clause to start defining the expression.

```
WITH ExplodeSource  -- specify the name of the result set we will query
AS                  
(                   -- wrap a SELECT statement in parentheses
  SELECT            -- this is the temporary result set you will query
    dc_id,
    to_date(date) AS date,
    EXPLODE (source)
  FROM
    DCDataRaw
)
SELECT             -- write a select statment to query the result set
  key,
  dc_id,
  date,
  value.description,  
  value.ip,
  value.temps,
  value.co2_level
FROM               -- this query is coming from the CTE we named
  ExplodeSource;  
```

<br>

# Create Table as Select (CTAS)

**CTEs** like those in the cell above are temporary and cannot be queried again. In the next cell, we demonstrate how you create a table using the common table expression syntax.

```
CREATE TABLE DeviceData                 
USING parquet
WITH ExplodeSource                       -- The start of the CTE from the last cell
AS (
  SELECT 
  dc_id,
  to_date(date) AS date,
  EXPLODE (source)
  FROM DCDataRaw
)
SELECT 
  dc_id,
  key device_type,                       
  date,
  value.description,
  value.ip,
  value.temps,
  value.co2_level
FROM ExplodeSource;
```

```
SELECT * FROM DeviceData
```

<br>

#

```
```

<br>

