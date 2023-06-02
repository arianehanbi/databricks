# databricks
Apache Spark SQL

<br>

# Create Table
- Create table using the Parquet file format. <br>
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

#### Checking NULLs
```
SELECT count(*) FROM tablename WHERE Description IS NULL;
```

#### COALESCE()
We can use it to **replace NULL values**. For all NULL values in the Description column, COALESCE() will replace the null with a value you include in the function.
```
COALESCE(colname, 0)
```

#### RAND()
Instead of simply returning the top rows, we can get a random sampling of rows using the function RAND() to return random rows.
```
SELECT * FROM tablename
ORDER BY RAND()
LIMIT 3;
```

#### TABLESAMPLE 
Sample the table. While accessing a random sample using the RAND() is a common way to retrieve a sample with other SQL dialects, Spark SQL includes a built-in function that you may want to use instead. The function, **TABLESAMPLE**, allows you to return a number of rows or a certain percentage of the data. In the cell directly below this one, we show that TABLESAMPLE can be used to access a specific number of rows. In the following, we show that it can be used to access a given percentage of the data. 
```
SELECT * FROM tablename TABLESAMPLE (5 ROWS)
SELECT * FROM tablename TABLESAMPLE (2 PERCENT) ORDER BY colname 
```

#### EXPLODE
Explode a nested object: We can observe from the output that the column contains a nested object with named key-value pairs. **EXPLODE** is used with arrays and elements of a map expression. When used with an array, it splits the elements into multiple rows. Used with a map, it splits the elements of a map into multiple rows and columns and uses the default names, key and value, to name the new columns. 
```
SELECT EXPLODE(colname) FROM tablename;
```

#### SPLIT()
This command *splits a string value around a specified character* and returns an array. An array is a list of values that you can access by position. This list is zero-indexed for the index of the first position is 0.
```
-- Date: 1/10/21 8:26
SPLIT(Date, "/")[0] month
SPLIT(Date, "/")[1] day
SPLIT(SPLIT(Date, " ")[0], "/")[2] year
```

#### LPAD()
It *inserts characters to the left of a string* until the string reachers a certain length. In this example, we use LPAD to insert a zero to the left of any value in the month or day column that is not two digits. 

```
LPAD(month, 2, 0) AS month
LPAD(day, 2, 0) AS day
```

#### CONCAT_WS()
It returns a concatenated string with a specified separator.
```
CONCAT_WS("/", month, day, year) sDate
TO_DATE(sDate, "MM/dd/yy") date
```

#### CAST()
It shows the timestamp as a human-readable time and date.
```
CAST(timeRecorded AS timestamp) 
CAST(UnitPrice AS DOUBLE)
```
```
TO_TIMESTAMP(timestamp, 'yyyy/MM/dd HH:mm:ss')
```

#### 

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
- **CTE starts with a WITH clause** 
- CTEs are supported in Spark SQL. A CTE provides a _temporary result_ set which you can then use in a SELECT statement. These are different from **temporary views** in that they cannot be used beyond the scope of a single query. In this case, we will use the CTE to get a closer look at the nested data without writing a new table or view. 
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
- CTEs like those in the cell above are temporary and cannot be queried again. 
- Here, we demonstrate how you create a table using the common table expression syntax.
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

# Higher-order Functions
- To deal with complex data types.
- we demonstrate how to use higher-order functions to transform, filter, and flag array data while preserving the original structure.

#### TRANSFORM
- It uses the provided function to transform all elements of an array. 
- SQL's built-in functions are designed to operate on a single, simple data type within a cell. They cannot process array values. Transform can be particularly useful _when you want to apply an existing function to each element in an array_. In this case, we want to rewrite all of the names in the categories column in lowercase.
```
TRANSFORM(input_array, iterator -> LOWER(iterator)) new_colname

SELECT 
  TRANSFORM(categories, cat -> LOWER(cat)) lwrCategories
FROM tablename
```

#### FILTER
- It allows us to create a new column based on whether or not values in an array meet a certain condition. 
- Let's say we want to remove the category "Engineering Blog" from all records in our categories column. I can use the FILTER function to create a new column that excludes that value from the each array.
```
FILTER (input_array, iterator -> condition) new_colname

SELECT
  categories,
  FILTER (categories, category -> category <> "Engineering Blog") woEngineering
FROM tablename
```

#### EXISTS
- It tests whether a statement is true for one or more elements in an array. 
- Let's say we want to flag all blog posts with "Company Blog" in the categories field. I can use the EXISTS function to mark which entries include that category.
```
EXISTS (input_array, iterator -> iterator = "Company Blog") new_colname

SELECT
  categories,
  EXISTS (categories, c -> c = "Company Blog") companyFlag
FROM tablename

```

#### REDUCE
- It is more advanced than TRANSFORM
- It takes two lambda functions. You can use it to reduce the elements of an array to a single value by merging the elements into a buffer, and applying a finishing function on the final buffer.
- We will use the reduce function to find an average value, by day, for our CO2 readings. Take a closer look at the individual pieces of the REDUCE function by reviewing the list below.
```
REDUCE(input_array, starting_point_for_buffer, (value, buffer_value) -> buffer_function, buffer_value ->(finishing_function))

REDUCE(co2_level, 0, (c, acc) -> c + acc, acc ->(acc div size(co2_level))) AS averageCo2Level
```

#### PIVOT
- A pivot table allows you to transform rows into columns and group by any data field.
1. The SELECT statement inside the parentheses in the input for this table
2. The first argument in the clause is an aggregate function and the column to be aggregated. Then, we specify the **pivot column in the FOR subclause**. The IN operator contains the pivot column values.
```
SELECT * FROM (
  SELECT device_type, averageCo2Level 
  FROM tablename
)
PIVOT (
  ROUND(AVG(averageCo2Level), 2) avg_co2 
  FOR device_type IN ('sensor-ipad', 'sensor-inest', 'sensor-istick', 'sensor-igauge')
  );
```

#### ROLLUPS
- Rollups are operators used with the GROUP BY clause. 
- Rollups are creating **subtotals** for a specific group of data. The subtotals introduce new rows.
```
SELECT 
  COALESCE(dc_id, "All data centers") AS dc_id,
  COALESCE(device_type, "All devices") AS device_type,
  ROUND(AVG(averageCo2Level)) AS avgCo2Level
FROM tablename
GROUP BY ROLLUP (dc_id, device_type)
ORDER BY dc_id, device_type;
```

#### CUBE
- CUBE is also an operator used with the GROUP BY clause. 
- Similar to ROLLUP, you can use CUBE to generate summary values for sub-elements grouped by column value. 
- CUBE is different than ROLLUP in that it will also generate **subtotals for all combinations of grouping columns** specified in the GROUP BY clause.
```
SELECT 
  COALESCE(dc_id, "All data centers") AS dc_id,
  COALESCE(device_type, "All devices") AS device_type,
  ROUND(AVG(averageCo2Level))  AS avgCo2Level
FROM tablename
GROUP BY CUBE (dc_id, device_type)
ORDER BY dc_id, device_type;
```

<br>

# PARTITIONING
- The result of this kind of partitioning is that the table is stored in separate files. This may speed up subsequent queries that can filter out certain partitions.
- **SHOW PARTITIONS** to see how your data is partitioned. In this case, we can verify that the data has been partitioned according to device type.
```
CREATE TABLE IF NOT EXISTS p_tablename
PARTITIONED BY (device_type)
AS
  SELECT
    dc_id,
    date,
    temps,
    REDUCE(temps, 0, (t, acc) -> t + acc, acc ->(acc div size(temps))) as avg_daily_temp_c,
    device_type
  FROM DeviceData;
```

```
SHOW PARTITIONS tablename
```
<br>

# WIDGET
Input widgets allow you to **add parameters to your notebooks and dashboards**. You can create and remove widgets, as well as retrieve values from them within a SQL query. Once created, they appear at the top of your notebook. You can design them to take user input as a:

- dropdown: provide a list of options for the user to select from
- text: user enters input as text
- combobox: Combination of text and dropdown. User selects a value from a provided list or input one in the text box.
- multiselect: Select one or more values from a list of provided values
```
CREATE WIDGET DROPDOWN selectedDeviceType DEFAULT "sensor-inest" CHOICES
SELECT DISTINCT device_type
FROM tablename
```

getArgument() to retrieve the current value selected in the widget.
```
SELECT 
  device_type,
  ROUND(AVG(avg_daily_temp_c),4) AS avgTemp,
  ROUND(STD(avg_daily_temp_c), 2) AS stdTemp
FROM p_tablename
WHERE device_type = getArgument("selectedDeviceType")
GROUP BY device_type
```

```
REMOVE WIDGET selectedDeviceType
```
<br>

# Window Functions
The OVER clause defines the 'window' in a window function! <br>
They calculate a return variable for every input row of a table based on a group of rows selected by the user, the frame. To use window functions, we need to mark that a function is used as **a window by adding an OVER clause** after a supported function in SQL. Within the OVER clause, you specify which rows are included in the frame associated with this window. <br>

In the example, the function we will use is `AVG`. We define the Window Specification associated with this function with `OVER(PARTITION BY ...)`. The results show that the average monthly temperature is calculated for a data center on a given date. The `WHERE` clause at the end of this query is included to show a whole month of data from a single data center.
```
SELECT 
  dc_id,
  month(date),
  avg_daily_temp_c,
  AVG(avg_daily_temp_c) OVER (PARTITION BY month(date), dc_id) AS avg_monthly_temp_c
FROM p_tablename
WHERE month(date)="8" AND dc_id = "dc-102";
```

```
WITH DiffChart AS
(
SELECT 
  dc_id,
  date,
  avg_daily_temp_c,
  AVG(avg_daily_temp_c) OVER (PARTITION BY month(date), dc_id) AS avg_monthly_temp_c  
FROM AvgTemps
)
SELECT 
  dc_id,
  date,
  avg_daily_temp_c,
  avg_monthly_temp_c,
  avg_daily_temp_c - ROUND(avg_monthly_temp_c) AS degree_diff
FROM DiffChart;
```

<br>

#

```
```

<br>

