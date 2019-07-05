package pv.aggregation

import cats.effect.IO
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import pv.aggregation.config.{AggregationConfig, ConfigReader}
import pv.aggregation.repository.SaveQueryResult
import pv.aggregation.service.OutputQueryService
import pv.common.output.table.PrecinctSquadSummary
import pv.common.util.IOSeqExecution.executeAll

object OutputQueryApp
  extends App
    with OutputQueryService
    with ConfigReader
    with SaveQueryResult {

  implicit val logger = Logger("query-service")

  val config = readConfig.unsafeRunSync()

  implicit val sparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate()

  def produceQueryResult(config: AggregationConfig)(implicit sparkSession: SparkSession): Seq[IO[Unit]] = {
    import sparkSession.implicits._
    Seq(
      saveResult(config.basePath, config.violationTimeTable, maxNumberOfViolationsPerTimeOfDay),
      saveResult[PrecinctSquadSummary, PrecinctSquadSummary](config.basePath, config.precinctSquadTable, precinctSquadSorted),
      saveResult(config.basePath, config.locationReasonTable, reasonByLocationSorted),
      saveResult(config.basePath, config.yearMonthTable, numberOfViolationPerMonthIn2015),
      saveTopKResult(config.basePath, config.stateTable,
        topKRegistrationStatesByViolations, config.topKResults),
      saveTopKResult(config.basePath, config.reasonTable, topKCommonReasons, config.topKResults),
      saveTopKResult(config.basePath, config.carColorTable, topKCarColors, config.topKResults),
      saveTopKResult(config.basePath, config.carMakeTable, topKCarMakes, config.topKResults),
      saveTopKResult(config.basePath, config.yearLocationTable, topKLocationsWithMostViolations, config.topKResults)
    )
  }

  executeAll(produceQueryResult(config))

}
