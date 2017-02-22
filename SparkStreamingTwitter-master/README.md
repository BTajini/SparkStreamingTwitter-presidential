# Spark Streaming Twitter - Presidential 2017

##Step 0 :
##### To build project use ```mvn package```
##### Target dir will contain two jars - with and without included dependencies

##Before running app edit _app.properties_ credentials to setup Twitter connection (after git clone)
```
cd SparkStreamingTwitter-presidential
cd SparkStreamingTwitter-master
cd /src/main/resources
vim app.properties
ctrl+a to insert
:w to save your modification
:q to quit
```
##### To execute application locally in Spark use :
```
spark-submit --master local[2] --class com.badr.app.TwitterStreamingCollector TwitterStreaming-1.0-SNAPSHOT-jar-with-dependencies.jar outputDir 10 1 100
```
##### Application parameters
#####Run streaming with the following parameters: <outputFile = /user/badr/tmp/tweets/> <batchIntervalSeconds = 10> " +
"<partitionsNum = 1> <numTweetsToCollect = 2000>

##Step 1 :
```
git clone https://github.com/BTajini/SparkStreamingTwitter-presidential
cd SparkStreamingTwitter-presidential
cd SparkStreamingTwitter-master
ll
mvn package
```

#####if you want to delete all the repository:
```
cd
rm -rf SparkStreamingTwitter-presidential
```

#####if the build is successful, so we can run our application using the command below :
#####be aware, must change the name of the user depending on your first name!

```
cd target
to submit your spark application:
```
#####to submit your spark application , be aware fot the last parameter, edit if you want the collected tweets <numTweetsToCollect = 2000 or 200 or 100> :
```
spark-submit --class com.badr.app.TwitterStreamingCollector \
--master yarn-client \
TwitterStreaming-1.0-SNAPSHOT-jar-with-dependencies.jar \
/user/badr/tmp/tweets/ 10 1 2000
```

##### tweets are downloaded to HDFS

```
hadoop fs -ls /user/badr/tmp/tweets/tweetsmerged/
```

#####if you want to If you want to view the collected tweets
```
vim part-00000
```

transfer your tweets to your home :
```
hadoop fs -get /user/badr/tmp/tweets/  /home/badr/
```

##Step 2 :

#####How to create and feed your external table on hive.
#####Run :
```
Hive
```

```
CREATE EXTERNAL TABLE twitter_presi(text STRING, latitude FLOAT, longitude FLOAT, created_at TIMESTAMP) ROW FORMAT
DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION '/user/badr/tmp/tweets/tweetsmerged/';
```
#####Next Step :
```
show tables;
select * from twitter_presi;
select count(*) from twitter_presi;
```
##Step 3 :

#####0.
```
spark-shell --master yarn --deploy-mode client
```
#####1.
```
val hc = new org.apache.spark.sql.hive.HiveContext(sc)
```
#####2.3.4
```
hc.sql("select * from twitter_presi").show
hc.sql("select * from twitter_presi").limit(2).show
```
#####or
```
hc.sql("select text from twitter_presi limit 2").collect().foreach(println)
```
#####5.
```
import hc.implicits._
import java.sql.Timestamp
```
#####6.
```
case class tweet(text: String, latitude: Option[Float], longitude: Option[Float], created_at: Option[Timestamp])  /option for nullable field
```
#####7.
```
val wordCounts = hc.sql("select * from twitter_presi").as[tweet_prezi].rdd
```
#####Some tests on our RDD
```
wordCounts.count()
wordCounts.collect().foreach(println)
wordCounts.take(10)

wordCounts.take(2).foreach(println)

wordCounts.groupBy("text").count().show()
```

##For MLIB  - Processing Data :

```
val sqlCtx = new org.apache.spark.sql.SQLContext(sc)
val texts = sqlCtx.sql("SELECT text from twitter_presi WHERE text IS NOT NULL").map(_.toString) // if it didn't work, run the command below
```
#####if sqlCtx didn't work, try the command below :
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
#####Some tweets from our collected tweets
```
val some_tweets = texts.take(50)
```
#####we create 10 clusters :
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
#####Now we can apply the model to a live twitter stream.

##For the deliver :
```
gedit sample.sql ```

#####to save on .sql our script and to save on .scala
```
gedit sample.scala
```

##BONUS:
#####to run our script saved as sample.sql directly on Hive:
```
hive –f /home/badr/sample.sql
```