package com.bkomar.app

import com.bkomar.utils.Utils
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j._

object TwitterStreamingCollector {
  private var numTweetsCollected = 0L //count for the tweet collected
  private var numTweetsToCollect = 30 // num of tweet to collect


  def main(args: Array[String]) {




    //number of args except filters
    val baseParamsCount = 3
    if (args.length < 3) {
      System.err.println("Run streaming with the following parameters: <outputPath> <batchIntervalSeconds> " +
        "<partitionsNum> <filtering keywords>")
      System.exit(1)
    }
    // Local directory for stream checkpointing (allows us to restart this stream on failure)

    //TODO bk move to utils
    val outputPath: String = args(0)   // outputPath = "tweets/
    val batchInterval: Int = args(1).toInt
    val partitionNum: Int = args(2).toInt
    //val keyWordsFilters: Seq[String] = args.takeRight(args.length - baseParamsCount)
    val keyWordsFilters = Seq("#LePen","#Macron","#Fillon","#JLM2017","#Hamon","#Mélenchon","#Sarkozy")


    println("Collector is executed with the filters: " + keyWordsFilters.mkString(Utils.hashTagSeparator))



    Utils.setUpTwitterOAuth

    val sparkConf = new SparkConf().setAppName("TwitterStreamingCollector")
    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))



    }
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
          rdd
            .map(t => (
              t.getUser.getName,
              t.getCreatedAt.toString,
              t.getText,
              t.getHashtagEntities.map(_.getText).mkString(Utils.hashTagSeparator)
              //            t.getRetweetCount  issue in twitter api, always returns 0
            ))
            .repartition(partitionNum)
            .coalesce(1, shuffle = true).saveAsTextFile(outputPath + "tweetsmerged")
            //.coalesce(1, shuffle = true).saveAsTextFile(outputPath + "tweets" + time.milliseconds.toString + ".txt")






        }


        //TODO bk add checkpointing
        //test for hive and sql context
      }
    })
    //ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()
  }
}
