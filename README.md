# Spark streaming using Twitter Auth for Presidential 2017 in France

> **Author:**
> [Badr Tajini](https://scholar.google.fr/citations?user=YuxT3tYAAAAJ&hl=en) <br>

> Update 2022 : The same data processing could be used for the 2022 presidential elections in France.

## Step 0 :
The target directory contains two Fat jar files
To build project use ```mvn package```

## Prepare environment
```
git clone https://github.com/BTajini/SparkStreamingTwitter-presidential
cd SparkStreamingTwitter-presidential
cd SparkStreamingTwitter-master
cd /src/main/resources
```
Create app.properties file to setup Twitter auth
```
cd /src/main/resources
nano app.properties
```
Add Twitter auth and save

```
twitter.oauth.consumer.key=<SECRET>
twitter.oauth.consumer.secret=<SECRET>
twitter.oauth.access.token=<SECRET>
twitter.oauth.access.token.secret=<SECRET>
```
Sample of app.properties file is available in this path
```
SparkStreamingTwitter-master/src/main/resources/app.properties
```
## USAGE
Execute application locally in Spark use :
```
spark-submit --master local[2] --class com.badr.app.TwitterStreamingCollector TwitterStreaming-1.0-SNAPSHOT-jar-with-dependencies.jar C:\Users\Badr\...\... 10 1 100
```
## Application parameters
Run streaming with the following parameters

```
<outputFile = /user/badr/tmp/tweets/> <batchIntervalSeconds = 10> <partitionsNum = 1> <numTweetsToCollect = 2000>
```

## Step 1 
```
git clone https://github.com/BTajini/SparkStreamingTwitter-presidential
cd SparkStreamingTwitter-presidential
cd SparkStreamingTwitter-master
ll
mvn package
```

If the build is successful, we can run our application using the command below 

Submit your spark application.
>  Be aware fot the last parameter '**2000**'.
>  
>  Edit if you want to collect more or less tweets **<numTweetsToCollect = 2000 or 200 or 100>** 
```
cd target

spark-submit --class com.badr.app.TwitterStreamingCollector \
--master yarn-client \
TwitterStreaming-1.0-SNAPSHOT-jar-with-dependencies.jar \
/user/badr/tmp/tweets/ 10 1 2000
```

Tweets are downloaded to HDFS

```
hadoop fs -ls /user/badr/tmp/tweets/tweetsmerged/
```

If you want to check the collected tweets
```
vim part-00000
```

Transfer your tweets to your home :
```
hadoop fs -get /user/badr/tmp/tweets/  /home/badr/
```

## Step 2 :

##### How to create and feed your external table on hive.

Run 
```
Hive
```

```
CREATE EXTERNAL TABLE twitter_presi(text STRING, latitude FLOAT, longitude FLOAT, created_at TIMESTAMP) ROW FORMAT
DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION '/user/badr/tmp/tweets/tweetsmerged/';
```
Next Step 
```
show tables;
select * from twitter_presi;
select count(*) from twitter_presi;
```
## Step 3 

##### 0.
```
spark-shell --master yarn --deploy-mode client
```
##### 1.
```
val hc = new org.apache.spark.sql.hive.HiveContext(sc)
```
##### 2.3.4
```
hc.sql("select * from twitter_presi").show
hc.sql("select * from twitter_presi").limit(2).show
```
or
```
hc.sql("select text from twitter_presi limit 2").collect().foreach(println)
```
##### 5.
```
import hc.implicits._
import java.sql.Timestamp
```
##### 6.
```
case class tweet(text: String, latitude: Option[Float], longitude: Option[Float], created_at: Option[Timestamp])  /option for nullable field
```
##### 7.
```
val wordCounts = hc.sql("select * from twitter_presi").as[tweet].rdd
```
Some tests on our RDD
```
wordCounts.count()
wordCounts.collect().foreach(println)
wordCounts.take(10)

wordCounts.take(2).foreach(println)

wordCounts.groupBy("text").count().show()
```

## For MLIB  - Processing Data

```
val sqlCtx = new org.apache.spark.sql.SQLContext(sc)
val texts = sqlCtx.sql("SELECT text from twitter_presi WHERE text IS NOT NULL").map(_.toString)
```
If sqlCtx didn't work, try the command below
```
val texts = hc.sql("SELECT text from twitter_presi WHERE text IS NOT NULL").map(_.toString)
```
```
import org.apache.spark.mllib.feature.HashingTF

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql._
```
```
val tf = new HashingTF(1000)
```
```
def featurize(s: String): Vector = { tf.transform(s.sliding(2).toSeq) }
```
```
val vectors = texts.map(featurize).cache()
```
```
val model = KMeans.train(vectors, 10, 20)
```
Some tweets from our collected tweets
```
val some_tweets = texts.take(50)
```
We create 10 clusters
```
for (i <- 0 until 10) {
println(s"\nCLUSTER $i:")
some_tweets.foreach { t =>
if (model.predict(featurize(t)) == i) {
println(t)
}
}
}
```

```
sc.makeRDD(model.clusterCenters, 10).saveAsObjectFile("/user/badr/tmp/tweets/myModelfortwitter")

```

Transfer your model to your home dir

```
hadoop fs -get /user/badr/tmp/tweets/myModelfortwitter  /home/badr/
```

Now we can apply the model to a live twitter stream

## Delivery :

Save in .sql format our script

```
gedit sample.sql
```

Save in .scala file our script

```
gedit sample.scala
```

## BONUS:
To run our script saved as sample.sql directly on Hive:

```
hive –f /home/badr/sample.sql
```
