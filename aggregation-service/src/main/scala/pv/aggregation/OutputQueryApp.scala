package pv.aggregation

import cats.effect.IO
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import pv.aggregation.config.{AggregationConfig, ConfigReader}
import pv.aggregation.repository.ShowQueryResult
import pv.aggregation.service.OutputQueryService
import pv.common.output.table.PrecinctSquadSummary
import pv.common.util.IOSeqExecution.executeSeq

object OutputQueryApp
  extends App
    with OutputQueryService
    with ConfigReader
    with ShowQueryResult {

  implicit val logger = Logger("query-service")

  val config = readConfig.unsafeRunSync()

  implicit val sparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate()

  def produceQueryResult(config: AggregationConfig)(implicit sparkSession: SparkSession): Seq[IO[Unit]] = {
    import sparkSession.implicits._
    Seq(
      showResult(config.basePath, config.violationTimeTable, maxNumberOfViolationsPerTimeOfDay),
      showResult[PrecinctSquadSummary, PrecinctSquadSummary](config.basePath, config.precinctSquadTable, precinctSquadSorted),
      showResult(config.basePath, config.locationReasonTable, reasonByLocationSorted),
      showResult(config.basePath, config.yearMonthTable, numberOfViolationPerMonthIn2015),
      showTopKResult(config.basePath, config.stateTable,
        topKRegistrationStatesByViolations, config.topKResults),
      showTopKResult(config.basePath, config.reasonTable, topKCommonReasons, config.topKResults),
      showTopKResult(config.basePath, config.carColorTable, topKCarColors, config.topKResults),
      showTopKResult(config.basePath, config.carMakeTable, topKCarMakes, config.topKResults),
      showTopKResult(config.basePath, config.yearLocationTable, topKLocationsWithMostViolations, config.topKResults)
    )
  }

  executeSeq(produceQueryResult(config))

}
