package com.bkomar.app

import com.bkomar.utils.Utils
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterStreamingCollector {
  private var numTweetsCollected = 0L //count for the tweet collected
  private var numTweetsToCollect = 20L // num of tweet to collect

  def main(args: Array[String]) {


    //number of args except filters
    val baseParamsCount = 3
    if (args.length < 4) {
      System.err.println("Run streaming with the following parameters: <checkpointDir> <outputPath> <batchIntervalSeconds> " +
        "<partitionsNum> <filtering keywords>")
      System.exit(1)
    }
    // Local directory for stream checkpointing (allows us to restart this stream on failure)
    val checkpointDir : String = args(0)//  checkpoint = "/user/badr/tmp/"
    //TODO bk move to utils
    val outputPath: String = args(1)   // outputPath = "tweets/
    val batchInterval: Int = args(2).toInt
    val partitionNum: Int = args(3).toInt
    //val keyWordsFilters: Seq[String] = args.takeRight(args.length - baseParamsCount)
    val keyWordsFilters = Seq("#LePen","#Macron","#Fillon","#JLM2017","#Hamon","#Mélenchon","#Sarkozy")


    println("Collector is executed with the filters: " + keyWordsFilters.mkString(Utils.hashTagSeparator))



    val sparkConf = new SparkConf().setAppName("TwitterStreamingCollector")
    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))

    Utils.setUpTwitterOAuth

    val hiveDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0")


    // A list of fields we want along with Hive column names and data types
    val fields: Seq[(Status => Any, String, String)] = Seq(
      (s => s.getText, "text", "STRING"),
      (s => s.getUser.getName, "user_name", "STRING"),
      (s => hiveDateFormat.format(s.getUser.getCreatedAt), "user_created_at", "TIMESTAMP"),

      // Break out date fields for partitioning
      (s => hiveDateFormat.format(s.getCreatedAt), "created_at", "TIMESTAMP")

    )
    // For making a table later, print out the schema
    val tableSchema = fields.map{case (f, name, hiveType) => "%s %s".format(name, hiveType)}.mkString("(", ", ", ")")
    println("Beginning collection. Table schema for Hive is: %s".format(tableSchema))

    // Remove special characters inside of statuses that screw up Hive's scanner.
    def formatStatus(s: Status): String = {
      def safeValue(a: Any) = Option(a)
        .map(_.toString)
        .map(_.replace("\t", ""))
        .map(_.replace("\"", ""))
        .map(_.replace("\n", ""))
        .map(_.replaceAll("[\\p{C}]","")) // Control characters
        .getOrElse("")

      fields.map{case (f, name, hiveType) => f(s)}
        .map(f => safeValue(f))
        .mkString("\t")
    }

    // Date format for creating Hive partitions
    val outDateFormat = outputBatchInterval match {
      case 60 => new java.text.SimpleDateFormat("yyyy/MM/dd/HH/mm")
      case 3600 => new java.text.SimpleDateFormat("yyyy/MM/dd/HH")
    }
    val twitterStream = TwitterUtils.createStream(ssc, None, keyWordsFilters)

    val frenchTweets = twitterStream.filter(_.getLang() == "fr")


    // Format each tweet
    val formattedStatuses = frenchTweets.map(s => formatStatus(s))

    // Group into larger batches
    val batchedStatuses = formattedStatuses.window(Seconds(outputBatchInterval), Seconds(outputBatchInterval))



    batchedStatuses.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (rdd.count() > 0) {
        if (numTweetsCollected > numTweetsToCollect) {
          System.exit(0)
          }
        else {
          numTweetsCollected += count
          println("Number of tweets received: " + count)
          rdd.repartition(partitionNum).coalesce(1, shuffle = true).saveAsTextFile(checkpointDir + outputPath + "tweetsmerged")
            //.coalesce(1, shuffle = true).saveAsTextFile(outputPath + "tweets" + time.milliseconds.toString + ".txt")


          import org.apache.hadoop.fs.Path
          val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
          fs.rename(new Path(checkpointDir + outputPath + "tweetsmerged/part-00000"), new Path(checkpointDir + outputPath + time.milliseconds.toString))
          fs.delete(new Path(checkpointDir + outputPath  + "tweetsmerged"), true)



        }


        //TODO bk add checkpointing
        //test for hive and sql context
      }
    })
    ssc.checkpoint(checkpointDir)
    ssc.start()
    //ssc.awaitTermination()
  }
}
