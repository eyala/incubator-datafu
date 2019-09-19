---
title: "Introducing spark-datafu"
author: Eyal Allweil
license: >
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
---

**_datafu-spark is a new module within Apache DataFu which provides utilities and UDF's for working with Apache Spark. It contains Spark versions of many of the same methods that exist within datafu-pig, as well as some entirely new functionality._**

For more than five years, datafu-pig has been an important collection of generic Apache Pig UDFs. Today we are happy to announce the availablity of datafu-spark, a new library containing utilities and UDFs for working with Apache Spark.

write an introduction here

---
<br>

**1\. Finding the most recent update of a given record — the _dedup_ (de-duplication) methods**

A common scenario in data sent to the HDFS — the Hadoop Distributed File System — is multiple rows representing updates for the same logical data. For example, in a table representing accounts, a record might be written every time customer data is updated, with each update receiving a newer timestamp. Let’s consider the following simplified example.

<br>
<script src="https://gist.github.com/eyala/65b6750b2539db5895738a49be3d8c98.js"></script>
<center>Raw customers’ data, with more than one row per customer</center>
<br>

We can see that though most of the customers only appear once, _julia_ and _quentin_ have 2 and 3 rows, respectively. How can we get just the most recent record for each customer? In datafu-pig, we would have used the _dedup_ macro. In Spark, we can use the _dedupWithOrder_ method.

```scala
import datafu.spark.DataFrameOps._

val customers = spark.read.format("csv").option("header", "true").load("customers.csv")

csv.dedupWithOrder($"id", $"date_updated".desc).show
```

Our result will be as expected — each customer only appears once, as you can see below:

<br>
<script src="https://gist.github.com/eyala/1dddebc39e9a3fe4501638a95f577752.js"></script>
<center>“Deduplicated” data, with only the most recent record for each customer (though not in order)</center>
<br>

add sentence about multiple field dedups.

There are two additional variants of _dedupWithOrder_ in datafu-spark. The _dedupWithCombiner_ method has similar functionality to _dedupWithOrder_, but uses a UDAF to utilize map side aggregation. _dedupTopN_ allows retaining more than one record for each key.


---
<br>

**2\. Doing skewed joins — the _joinSkewed_ and _broadcastJoinSkewed_ methods**

The _joinSkewed_ method should be used when the right data frame is relatively small but still too big to fit in memory for a map side broadcast join.

The result will be all the records from our original table for customers 2, 4 and 6. Notice that the original row structure is preserved, and that customer 2 —_ julia_ — has two rows, as was the case in our original data. This is important for making sure that the code that will run on this sample will behave exactly as it would on the original data.

<br>
<script src="https://gist.github.com/eyala/28985cc0e3f338d044cc5ebb779f6454.js"></script>
<center>Only customers 2, 4, and 6 appear in our new sample</center>
<br>

---
<br>

**3\. Joining a table with a numeric column with a table with a range - the _joinWithRange_ method**

 * Helper function to join a table with point column to a table with range column.
    * For example, join a table that contains specific time in minutes with a table that contains time ranges.
    * The main problem this function addresses is that doing naive explode on the ranges can result in a huge table.
    * requires:
    * 1. point table needs to be distinct on the point column. there could be a few corresponding ranges to each point,
    *    so we choose the minimal range.
    * 2. the range and point columns need to be numeric.
    *
    * TIMES:
    * +-------+
    * |time   |
    * +-------+
    * |11:55  |
    * +-------+
    *
    * TIME RANGES:
    * +----------+---------+----------+
    * |start_time|end_time |desc      |
    * +----------+---------+----------+
    * |10:00     |12:00    | meeting  |
    * +----------+---------+----------+
    * |11:50     |12:15    | lunch    |
    * +----------+---------+----------+
    *
    * OUTPUT:
    * +-------+----------+---------+---------+
    * |time   |start_time|end_time |desc     |
    * +-------+----------+---------+---------+
    * |11:55  |10:00     |12:00    | meeting |
    * +-------+----------+---------+---------+
    * |11:55  |11:50     |12:15    | lunch   |
    * +-------+----------+---------+---------+


**4\. Counting distinct records, but only up to a limited amount — the _CountDistinctUpTo_ UDAF**

Sometimes our analytical logic requires us to filter out accounts that don’t have enough data. For example, we might want to look only at customers with a certain small minimum number of transactions. This is not difficult to do in Pig; you can group by the customer’s id, count the number of distinct transactions, and filter out the customers that don’t have enough.

Let’s use following table as an example:

<br>
<script src="https://gist.github.com/eyala/73dc69d0b5f513c53c4dac72c71daf7c.js"></script>
<br>

You can use the following “pure” Pig script to get the number of distinct transactions per name:

```pig
data = LOAD 'transactions.csv' USING PigStorage(',') AS (name: chararray, transaction_id:int);

grouped = GROUP data BY name;

counts = FOREACH grouped {
 distincts = DISTINCT data.transaction_id;
 GENERATE group, COUNT(distincts) AS distinct_count;
 };

DUMP counts;
```

This will produce the following output:

<br>
<script src="https://gist.github.com/eyala/a9cd0ffb99039758f63b9d08c40b1124.js"></script>
<br>

Note that Julia has a count of 1, because although she has 2 rows, they have the same transaction id.

However, accounts in PayPal can differ wildly in their scope. For example, a transactions table might have only a few purchases for an individual, but millions for a large company. This is an example of data skew, and the procedure I described above would not work effectively in such cases. This has to do with how Pig translates the nested foreach statement — it will keep all the distinct records in memory while counting.

In order to get the same count with much better performance, you can use the _CountDistinctUpTo_ UDF. Let’s look at the following Pig script, which counts distinct transactions up to 3 and 5:

```pig
REGISTER datafu-pig-1.5.0.jar;

DEFINE CountDistinctUpTo3 datafu.pig.bags.CountDistinctUpTo('3');
DEFINE CountDistinctUpTo5 datafu.pig.bags.CountDistinctUpTo('5');

data = LOAD 'transactions.csv' USING PigStorage(',') AS (name: chararray, transaction_id:int);

grouped = GROUP data BY name;

counts = FOREACH grouped GENERATE group,CountDistinctUpTo3($1) as cnt3, CountDistinctUpTo5($1) AS cnt5;

DUMP counts;
```

This results in the following output:

<br>
<script src="https://gist.github.com/eyala/19e22fb251fe2222b3ccea6f78e37a85.js"></script>
<br>

Notice that when we ask _CountDistinctUpTo_ to stop at 3, _quentin_ gets a count of 3, even though he has 4 transactions. When we use 5 as a parameter to _CountDistinctUpTo_, he gets the actual count of 4.

In our example, there’s no real reason to use the _CountDistinctUpTo_ UDF. But in our “real” use case, stopping the count at a small number instead of counting millions saves resources and time. The improvement is because the UDF doesn’t need to keep all the records in memory in order to return the desired result.

---
<br>

**5\. Calling Python code from Scala, and Scala code from Python — the ScalaPythonBridge**

# datafu-spark

datafu-spark contains a "Scala-Python Bridge" which allows calling arbitrary Scala code from Python, and Python code from Scala.

For example, in order to call the spark-datafu API's from Pyspark, you can do the following (tested on a Hortonworks vm)

First, call pyspark with the following parameters

```bash
export PYTHONPATH=datafu-spark_2.11_2.3.0-1.5.0-SNAPSHOT.jar

pyspark  --jars datafu-spark_2.11_2.3.0-1.5.0-SNAPSHOT.jar --conf spark.executorEnv.PYTHONPATH=datafu-spark_2.11_2.3.0-1.5.0-SNAPSHOT.jar
```

The following is an example of calling the Spark version of the datafu _dedupWithOrder_ method

```python
from pyspark_utils.df_utils import PySparkDFUtils

df_utils = PySparkDFUtils()

df_people = sqlContext.createDataFrame([
...     ("a", "Alice", 34),
...     ("a", "Sara", 33),
...     ("b", "Bob", 36),
...     ("b", "Charlie", 30),
...     ("c", "David", 29),
...     ("c", "Esther", 32),
...     ("c", "Fanny", 36),
...     ("c", "Zoey", 36)],
...     ["id", "name", "age"])

func_dedup_res = df_utils.dedup_with_order(dataFrame=df_people, groupCol=df_people.id,
...                              orderCols=[df_people.age.desc(), df_people.name.desc()])

func_dedup_res.registerTempTable("dedup")

func_dedup_res.show()
```

This should produce the following output

<pre>
+---+-----+---+
| id| name|age|
+---+-----+---+
|  c| Zoey| 36|
|  b|  Bob| 36|
|  a|Alice| 34|
+---+-----+---+
</pre>


I hope that I’ve managed to explain how to use our new Spark contributions to DataFu. You can find all of the files used in this post by clicking the GitHub gists.

---

A version of this post has appeared in the [PayPal Engineering Blog.](https://medium.com/paypal-engineering/blahblahblah)

