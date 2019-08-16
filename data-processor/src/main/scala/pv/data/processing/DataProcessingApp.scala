package pv.data.processing

import cats.effect.IO
import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import pv.data.processing.analytics.AnalyticsService

object DataProcessingApp
  extends App
    with StreamingService
    with AnalyticsService {

  implicit val logger = Logger("data-processing-service")

  val conf = new SparkConf()
    .setAppName("data-processing")
    .setMaster("local[3]")
   // .set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
   // .set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

  implicit def sparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  def createStreamingContext(): StreamingContext = {
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint(config.basePath + "/checkpoint/")
    ssc
  }

  val streamingCtx = StreamingContext.getOrCreate(
    config.basePath + "/checkpoint/",
    createStreamingContext
  )

  Seq(
    processStream(config, precinctSquadSummary, config.precinctSquadTable),
    processStream(config, annualSummary, config.yearMonthTable),
    processStream(config, violationTimeSummary, config.violationTimeTable),
    processStream(config, carMakeSummary, config.carMakeTable),
    processStream(config, carColorSummary, config.carColorTable),
    processStream(config, stateSummary, config.stateTable),
    processStream(config, annualLocationSummary, config.yearLocationTable),
    processStream(config, locationReasonSummary, config.locationReasonTable)
  ).par
    .foreach {
    _.handleErrorWith { e =>
      logger.error(e.getMessage)
      IO.raiseError(e)
    }.unsafeRunSync()
  }

  streamingCtx.start()
  streamingCtx.awaitTermination()
  streamingCtx.stop(
    stopSparkContext = false,
    stopGracefully = false
  )
}
