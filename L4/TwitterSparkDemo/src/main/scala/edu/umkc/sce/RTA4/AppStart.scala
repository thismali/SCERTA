package edu.umkc.sce.RTA4

/**
 * Created by mali on 9/23/2015.
 */

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object AppStart {

  def main(args: Array[String]) {

    val filters = args

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials

    System.setProperty("twitter4j.oauth.consumerKey", "*******************************")
    System.setProperty("twitter4j.oauth.consumerSecret", "*******************************")
    System.setProperty("twitter4j.oauth.accessToken", "***********************************")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "*******************************")

    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("TwitterStreamingRealTime").setMaster("local[*]")
    //Create a Streaming Context with 2 second window
    val sc = new StreamingContext(sparkConf, Seconds(2))
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets
    val stream = TwitterUtils.createStream(sc, None, filters)
    //stream.print()


    //get the tweeting city through the Scala options (making sure NULL is transformed)
    val cities = stream.flatMap(status => this.getCityOrUnknown(Option(status)).split(" "))

    val topCities = cities.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (city, count) => (count, city)}
      //.map(city => (city, 1))
      .reduceByKey((a, b) => a + b)
      .transform(_.sortByKey(false))


    topCities.foreachRDD(rdd => {
      val topList = rdd.take(20)
      println("\nThese are the current top tweeting cities: (%s total):".format(rdd.count()))
      topList.foreach{case (count, city) => println("%s (%s tweets)".format(city, count))}
      topCities.saveAsTextFiles("src/main/resources/output/OutputFile.txt")
    })

    topCities.foreachRDD(rdd => {
      val topList = rdd.take(1)
      edu.umkc.sce.RTA4.Dispatcher.dispatchCommand(rdd.count() +" is the total number of tweets analyzed")
      println("\nThese are the current top tweeting cities: (%s total):".format(rdd.count()))
      topList.foreach{case (count, city) => println("%s (%s tweets)".format(city, count))}

    })


    sc.start()

    sc.awaitTermination()
  }

  //gets city from status. If tweet Place is identified, get the city, otherwise get user's city
  def getCityOrUnknown(s: Option[twitter4j.Status]):String = {


    var rtn: String = "Unknown"


    val loc = s.get.getPlace()
    if(loc.!=(null)) {
      rtn = s.get.getPlace.getFullName()
    }
    else {
      val user = s.get.getUser();

      if(user.!=(null)) rtn =  s.get.getUser.getLocation()

    }

    if(this.containsNoSpecialChars(rtn)) {
      //println("found city: " + rtn)

      return rtn
    }
    else return "Unknown"
  }

  //used to discard chunk names for city
  def containsNoSpecialChars(string: String): Boolean = {
    val pattern = "^[a-zA-Z0-9]*$".r
    return !string.isEmpty && pattern.findAllIn(string).mkString.length == string.length
  }
}
