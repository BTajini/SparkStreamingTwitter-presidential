package com.bkomar.app

import com.bkomar.utils.Utils
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j._

object TwitterStreamingCollector {
  private var numTweetsCollected = 0L //count for the tweet collected
  private var numTweetsToCollect = 20L // num of tweet to collect


  def main(args: Array[String]) {

    // Size of output batches in seconds


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


    Utils.setUpTwitterOAuth

    val sparkConf = new SparkConf().setAppName("TwitterStreamingCollector")
    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))


    val twitterStream = TwitterUtils.createStream(ssc, None, keyWordsFilters)
    val frenchTweets = twitterStream.filter { status =>
      Option(status.getUser).flatMap[String] {
        u => Option(u.getLang)
      }.getOrElse("").startsWith("fr")
    }




    frenchTweets.foreachRDD((rdd, time) => {
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
