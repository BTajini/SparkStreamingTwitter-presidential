package com.bkomar.utils

import java.util.Properties
import scala.io.Source


object Utils {

  def hashTagSeparator = "|"

  /**
   * Set up Twitter oAuth connection properties
   */
  def setUpTwitterOAuth() = {
    val props = readProperties("app.properties")
    System.setProperty("twitter4j.oauth.consumerKey",
      props.getProperty("twitter.oauth.consumer.key"))
    System.setProperty("twitter4j.oauth.consumerSecret",
      props.getProperty("twitter.oauth.consumer.secret"))
    System.setProperty("twitter4j.oauth.accessToken",
      props.getProperty("twitter.oauth.access.token"))
    System.setProperty("twitter4j.oauth.accessTokenSecret",
      props.getProperty("twitter.oauth.access.token.secret"))

//    System.setProperty("twitter4j.http.connectionTimeout", "50000")
//    System.setProperty("twitter4j.http.readTimeout", "220000")
  }

  /**
   * Loads properties from app classpath
   * @param fileName properties file name
   * @return properties
   */
  def readProperties(fileName: String) : Properties = {
    val url = getClass.getResource("/".concat(fileName))
    val properties = new Properties()
    if (url != null) {
      try {
        val reader = Source.fromURL(url).bufferedReader()
        properties.load(reader)
        reader.close()
      } catch {
        case ex: Exception =>
          System.err.println("Application properties can not be loaded." + ex.getMessage)
          System.exit(1)
      }
    }
    return properties
  }
}
