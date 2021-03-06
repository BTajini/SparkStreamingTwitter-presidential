package com.badr.app

import com.badr.utils.Utils
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j._
import java.io._

object TwitterStreamingCollector {
  private var numTweetsCollected = 0L//count for the tweet collected
  // num of tweet to collect


  def main(args: Array[String]) {

    //number of args except filters
    val baseParamsCount = 3
    if (args.length < 4) {
      System.err.println("Run streaming with the following parameters: <outputFile = /user/badr/tmp/tweets/> <batchIntervalSeconds = 10> " +
        "<partitionsNum = 1> <numTweetsToCollect = 2000>")
      System.exit(1)
    }
    // Local directory for stream checkpointing (allows us to restart this stream on failure)


    val outputFile: String = args(0)   // outputFile = "tweets/
    val batchInterval: Int = args(1).toInt
    val partitionNum: Int = args(2).toInt
    val numTweetsToCollect: Int = args(3).toInt
    //val keyWordsFilters: Seq[String] = args.takeRight(args.length - baseParamsCount) // if we want add keywords by parameters
    val keyWordsFilters = Seq("#LePen","#Macron","#Fillon","#JLM2017","#Hamon","#Mélenchon","#Sarkozy")


    println("Collector is executed with the filters: " + keyWordsFilters.mkString(Utils.hashTagSeparator))



    Utils.setUpTwitterOAuth

    val sparkConf = new SparkConf().setAppName("TwitterStreamingCollector")
    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))

    val hiveDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0")

    val fields: Seq[(Status => Any, String, String)] = Seq(

      (s => s.getText, "text", "STRING"),
      (s => Option(s.getGeoLocation).map(_.getLatitude()).getOrElse(""), "latitude", "FLOAT"),
      (s => Option(s.getGeoLocation).map(_.getLongitude()).getOrElse(""), "longitude", "FLOAT"),
      // Break out date fields for partitioning
      (s => hiveDateFormat.format(s.getCreatedAt), "created_at", "TIMESTAMP")
    )

    def formatStatus(s: Status): String = {
      def safeValue(a: Any) = Option(a)
        .map(_.toString)
        .map(_.replace("\t", ""))
        .map(_.replace("\"", ""))
        .map(_.replace("[\r\n]", ""))
        .map(_.replace(",", "'"))
        .map(_.replaceAll("[\\p{C}]","")) // Control characters
        .getOrElse("")

      fields.map{case (f, name, hiveType) => f(s)}
        .map(f => safeValue(f))
        .mkString("|")
    }

    val twitterStream = TwitterUtils.createStream(ssc, None, keyWordsFilters)

    val frenchTweets = twitterStream.filter { status =>
      Option(status.getUser).flatMap[String] {
        u => Option(u.getLang)
      }.getOrElse("").startsWith("fr")
    }

    // Format each tweet
    val formattedStatuses = frenchTweets.map(s => formatStatus(s))



    formattedStatuses.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (rdd.count() > 0) {
          println("Number of tweets received: " + count)
          numTweetsCollected += count
          val outputRDD = rdd.repartition(partitionNum)
          outputRDD.saveAsTextFile(outputFile + "tweetsmerged")
          if (numTweetsCollected > numTweetsToCollect) {
            System.exit(0)
            }
            //.coalesce(1, shuffle = true).saveAsTextFile(outputPath + "tweets" + time.milliseconds.toString + ".txt")

        }


        //TODO  add checkpointing
        //TODO condition not null GetgeoLocation
      }
    )
    //ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()
  }
}
