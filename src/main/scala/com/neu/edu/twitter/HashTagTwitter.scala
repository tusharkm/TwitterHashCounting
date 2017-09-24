package com.neu.edu.twitter

/** Listens to a stream of Tweets and keeps track of the most popular
  *  hashtags
  */
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import scala.io.Source
import com.google.common.base.CharMatcher
import org.apache.log4j.{Level, Logger}
object HashTagTwitter {

  /**  ERROR messages get logged to avoid log spam. */
  def setupLogging()={

    val rootLogger=Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

  }


  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter() ={
    for(line<-Source.fromFile("./twitter.txt").getLines()){
      val fields=line.split(" ")
      if(fields.length==2)
        {
          System.setProperty("twitter4j.oauth."+fields(0),fields(1))
        }
    }

  }

  def main(args: Array[String]): Unit =
  {
    setupTwitter()

    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "HashTagTwitter", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

      // Creating Dstream from twitter using the Streamig context
      val tweets = TwitterUtils.createStream(ssc, None)



    // Now extract the text of each status update into DStreams using map()


    val statuses=tweets.map(status=>status.getText())

    // Remove  each word into a new DStream
    val tweetWords= statuses.flatMap(tweetText=>tweetText.split(" "))


    // Now eliminate anything that's not a hashtag
    val hashTags=tweetWords.filter(word=>word.startsWith("#"))

    // Map each hashtag to a key/value pair of (hashtag, 1)
    val hashtagKeyValues=hashTags.map(hashTags=>(hashTags,1))

    // Count them up over a 5 minute window sliding every one second
    val hashTagCounts = hashtagKeyValues.reduceByKeyAndWindow(_+_,_-_,Seconds(300),Seconds(1))

    //Sort the values by count
    val sortedResult=hashTagCounts.transform(rdd=>rdd.sortBy(x=>x._2,false))

    // Print the top 10
    sortedResult.print

    // Set a checkpoint directory, and kick it all off
    // To watch this all day!

    ssc.checkpoint("./Checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
