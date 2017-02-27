package com.geraud

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by geraud on 27/02/17.
  */
object AirportStream {

  case class Flight(date: String, year: String, month: String, day: String, dayOfWeek: String, uniqueCarrier: String, arrDelay: Long, depDelay: Long)
  case class Delay(carrier: String, delay: Long)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("App name")
    val ssc = new StreamingContext(conf, Seconds(5))

    val filestream = ssc.textFileStream("/home/geraud/sample-data")
    val delayByCarrier = filestream.flatMap(line => {
      val values = line.split(",")
      try {
        assert(values(0) != "Year")
        List(Flight(s"${values(0)}-${values(1)}-${values(2)}", values(0), values(1), values(2), values(3), values(8), values(14).toLong, values(15).toLong))
      } catch {
        case e: Throwable => {
          //          println("Header row")
          List()
        }
      }
    })
      .map(flight => (flight.uniqueCarrier, flight.arrDelay))
      .reduceByKey((f1, f2) => f1 + f2)

    delayByCarrier.print()
    delayByCarrier.repartition(1).saveAsTextFiles("/home/geraud/results/sample-output", "txt")
    ssc.start()
    ssc.awaitTerminationOrTimeout(60000L)
  }
}
