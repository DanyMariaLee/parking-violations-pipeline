package pv.data.processing

import akka.actor.ActorSystem
import org.apache.spark.sql.SparkSession

object DataProcessingApp extends App with StreamingService {

  implicit val sparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate()

 // val hdConf = streamingContext.sparkContext.hadoopConfiguration
 // hdConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
 // hdConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

  implicit val actorSystem = ActorSystem("data-processing-service")

  sparkSession.sparkContext.setLogLevel("ERROR")
  streamingContext.sparkContext.setLogLevel("ERROR")

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
    .foreach(
      _.unsafeRunSync()
    )

  streamingContext.start()
  streamingContext.awaitTermination()
  streamingContext.stop(
    stopSparkContext = false,
    stopGracefully = false
  )
}
