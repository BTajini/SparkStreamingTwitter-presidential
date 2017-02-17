# Spark Streaming Twitter Collector

##### To build project use ```mvn package``` 
##### Target dir will contain two jars - with and without included dependencies

Before running app edit _app.properties_ to setup Twitter connection

##### To execute application in Spark use 
```
spark-submit --master local[2] --class com.bkomar.app.TwitterStreamingCollector TwitterStreaming-1.0-SNAPSHOT-jar-with-dependencies.jar outputDir 5 1
```
##### Application parameters
<outputPath> <batchIntervalSeconds> <partitionsNum> <filtering keywords>