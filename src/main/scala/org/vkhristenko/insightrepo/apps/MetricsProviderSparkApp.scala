package org.vkhristenko.insightrepo.apps

import org.vkhristenko.insightrepo.common._
import org.vkhristenko.insightrepo.login._
import org.vkhristenko.insightrepo.frame._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.expressions.scalalang._

import java.io._
import scala.io.Source
import scala.io.Codec
import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction

object MetricsProviderSparkApp {
  def main(args: Array[String]) {
    val inputFileName = args(0)
    val outputF1 = args(1)
    val outputF2 = args(2)
    val outputF3 = args(3)
    val outputF4 = args(4)
    val outputF3Like = args(5)
    println(s"input = $inputFileName")

    println(s"Start...")
    val spark = SparkSession.builder()
      .master("local")
      .appName("Insight Metrics Provider")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    
    val df = spark.sqlContext.read.text("file:" + inputFileName).as[String]
      .map(parseWithRE(_))

    // feature 1
    val dfF1 = df.groupBy(df("host")).count.sort($"count".desc)
    val resultF1 = dfF1.as[(String, Long)].take(10)
    val outF1 = new PrintWriter(new File(outputF1))
    for (t <- resultF1) outF1.write(s"${t._1},${t._2}\n")
    outF1.close

    //
    // feature 2
    // TODO: Not clear what you mean by frequency of access to a resource!
    //
    val dfGroupedByResource = df.filter({e: Event => e.request.method=="GET" || e.request.method=="POST"}).groupByKey({
      x: Event => x.request.resource
    })
    val resultF2 = dfGroupedByResource.agg(typed.sum({e: Event => e.bytes}))
      .toDF("resource", "bytes")
      .sort($"bytes".desc).as[(String, Double)].take(10)
    val outF2 = new PrintWriter(new File(outputF2))
    for (t <- resultF2) outF2.write(s"${t._1}\n")
    outF2.close

    // feature 3 - like... no sliding window
    // TODO: Why do we want sliding window???
    val dfGroupedByHour = df.groupByKey({
      e: Event => e.timestamp.toSeconds/3600L
    }).count
    val resultF3 = dfGroupedByHour.map({
      x: (Long, Long) => (TimeStamp.fromSeconds(x._1 * 3600L).str, x._2)
    }).sort($"_2".desc).take(10)
    val outF3Like = new PrintWriter(new File(outputF3Like))
    for (t <- resultF3) outF3Like.write(s"${t._1},${t._2}\n")
    outF3Like.close

    // features 4
    val manager = new LoginManager(outputF4)
    val decoder = Codec.UTF8.decoder.onMalformedInput(CodingErrorAction.IGNORE)
    var timeFrame = new TimeFrame(TimeStamp.fromSeconds(0L))
    // NOTE: getLines returns an iterator, not the whole List!
    var counter = 0L
    for (line <- Source.fromFile(inputFileName)(decoder).getLines) {
      val event = parseWithRE(line)
      manager.register(event)
      // NOTE: important to reinitialize with the starting reference time!
      if (counter==0L)
        timeFrame = new TimeFrame(event.timestamp)
      timeFrame.register(event)
      counter += 1L
    }
    // close the printer for F4
    manager.printer.close

    // finalize the F3 stuff
    timeFrame.flush(timeFrame.window)
    val outF3 = new PrintWriter(new File(outputF3))
    for (x <- timeFrame.busiest) outF3.write(s"${x._1},${x._2}\n")
    outF3.close
    
    // stop the Spark Context
    spark.stop()
    println(s"Stop...")
  }
}
