# HDFS parquet => Spark DataFrame

## Performance tricks 

The main idea was to have a look at the most common time-consuming operations with DataFrames and see if we can achieve performance improvement by only changing the way how we read the parquet initially.

*Hint*: The way you read parquet files on HDFS into DataFrame may define for how long your job runs, especially if your parquet is being partitioned.

## What are we looking at?

Reading parquet file from HDFS into Spark [DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes)
```scala
spark.read.parquet("/path/to/file.parquet") 
```

## What did I compare?

There are multiple ways how you can declare the variables in Scala, and apparently that may affect the timings of your Spark job if you work with large parquet files.

I compared different ways how you can declare the variable that you'll refer to your parquet later on:

[1] Standard _val_ 
```scala
val myDf = spark.read.parquet("/path/to/file.parquet") 
```

[2] Lazy _val_
```scala
lazy val myLazyDf = spark.read.parquet("/path/to/file.parquet") 
```

[3] Lazy _val_ with DataFrame wrapped in _Try_
```scala
import scala.util.Try
lazy val myLazyTryDf = Try(spark.read.parquet("/path/to/file.parquet"))
```

## What parquet files did I use for comparison?

I have used multiple parquet files: small (< 1 GB), average (< 100GB) and large (~1TB).

Small parquet hasn't been partitioned. 

Average and large files have had the partitions as of below: 

```shell script
+--- parquet-name
  |
  + --- top-parttition-value-1
  |  |
  |  +--- second-partition-value-1-1
  |  |
  |  +--- second-partition-value-1-2
  |  |
  |  +--- etc...
  |
  + --- top-parttition-value-2
  |  |
  |  +--- second-partition-value-2-1
  |  |
  |  +--- second-partition-value-2-2
  |  |
  |  +--- etc...
  |
  +--- etc...
```

## What kind of operations/actions did I perform on DataFrame after reading?

The following set of operations/actions has been applied to each parquet independently, and run time has been measured. 

* Read parquet from HDFS into Spark DataFrame
* Get the distinct counts for each group of two columns
* Get the max count for one of the columns from the previous grouping
* Get the min from the previous grouping

```scala
// What I did with DataFrame
import org.apache.spark.sql.DataFrame

function doSomething(groupByCol1: String, groupByCol2: String, countCol: String)(df: DataFrame): Long = 
    df.groupBy(groupByCol1, groupByCol2)
      .agg(countDistinct(countCol) as "counts")
        .groupBy(groupByCol1).agg(max("counts") as "res")
           .select(min("res"))
```

That didn't include time of starting/ending Spark or any actions before or after the code snippet

## How did I measure the run time?

For benchmarking I've used the function below. 
Please, be aware that Spark timing may have 1-2 minutes variance for the same job, and therefore it's important to run the test multiple times and work off averaged results.

```scala
// Benchmarking function

import scala.concurrent.duration._

def timer[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name	
    val t1 = System.nanoTime()
    val duration = Duration((t1 - t0), NANOSECONDS)
    println(s"Elapsed time: ${duration.toSeconds} seconds")
     result
}
```

## How did I compare the run times? What was the comparison about?

I performed the same set of operations/actions on each of the DataFrames (small, average and large) and measured how long it took. 

I've run the same code snippet for about 8-10 times and get the averaged results, and I compared the outcomes.

The operations/actions performed on the DataFrames _did *NOT*_ change. The only difference was the way I read the parquet. Due to the parquet reading itself being lazy in Spark, it doesn't matter if we include this line into timer or not, even though I tested both options.

I have tested two groupings: 
* Group by columns corresponding to top partition, and then second partition
* Group in reverse order, by columns corresponding to second partition, and then top partition

Testing use-cases for the groupings that don't involve partitioning columns are not presented below because I was not able to produce consistent results. 

### Standard _val_ 
#### Top partition, second partition
```scala
val myDf = spark.read.parquet("/path/to/file.parquet") 
timer {
  doSomething(topPartition, secondPartition, aggForCountsColName)(myDf)
    .collect
    .toList
      .foreach(println)
}
```

#### Second partition, top partition
```scala
val myDf = spark.read.parquet("/path/to/file.parquet") 
timer {
  doSomething(secondPartition, topPartition, aggForCountsColName)(myDf)
    .collect
    .toList
      .foreach(println)
}
```

### Lazy _val_
#### Top partition, second partition
```scala
lazy val myLazyDf = spark.read.parquet("/path/to/file.parquet") 
 timer {
   doSomething(topPartition, secondPartition, aggForCountsColName)(myLazyDf)
     .collect
     .toList
       .foreach(println)
 }
```

#### Second partition, top partition
```scala
lazy val myLazyDf = spark.read.parquet("/path/to/file.parquet") 
timer {
   doSomething(secondPartition, topPartition, aggForCountsColName)(myLazyDf)
     .collect
     .toList
       .foreach(println)
}
```

### Lazy _val_ with DataFrame wrapped in _Try_
#### Top partition, second partition
```scala
import scala.util.Try

lazy val myLazyTryDf = Try(spark.read.parquet("/path/to/file.parquet"))
timer {
    for {
        df <- myLazyTryDf.map(doSomething(topPartition, secondPartition, aggForCountsColName))
    } yield {
        df
          .collect
          .toList
            .foreach(println)
    }
}
```

#### Second partition, top partition
```scala
import scala.util.Try

lazy val myLazyTryDf = Try(spark.read.parquet("/path/to/file.parquet"))
timer {
    for {
        df <- myLazyTryDf.map(doSomething(secondPartition, topPartition, aggForCountsColName))
    } yield {
        df
          .collect
          .toList
            .foreach(println)
    }
}
```

## Results

Based on average run time

### Small DataFrame (< 1 GB), no partitioning

No differences noted, all run times were more or less the same.

### Average DataFrame (< 100 GB), partitioned

Lazy val [2] took either the same time or longer than original read [1], for both grouping.

Lazy val in Try [3] was *5-12%* faster than original read [1], and in some cases was twice as fast as regular lazy val [2].

_Winner_: Lazy val in Try [3]

### Large DataFrame (at least 500 GB, tested on ~1 TB), partitioned

* Grouping by top partition first, and then second partition

Both lazy val [2] and lazy val in Try [3] took much less time, showing improvement in about *30-50%*. In some cases it was half time comparing to traditional read [1].

* Grouping by second partition first, and then top partition

Lazy val [2] performed better in most of the runs cases but also had much more variance in results, reducing the run time up to *5-20%* in comparison with original read [1].

Lazy val in Try [3] showed less impressive but more stable *10-15%* improvement comparing to original read [1].

_Winner_: No clear winner, more like a tie

### TL;DR Summary

Use lazy variables when reading your parquet files, it will improve the performance in most of the cases.

My personal preference is to use `lazy val in Try` because it shows more stable results on every size of DataFrame and order of grouping columns, whereas `lazy val` may not perform well on some DataFrames and only shines on really big data (> 500 GB).