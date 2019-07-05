package pv.aggregation

import cats.effect.IO
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import pv.aggregation.config.{AggregationConfig, ConfigReader}
import pv.aggregation.repository.{DatasetReader, DatasetWriter, SaveQueryResult, UpdateDataset}
import pv.aggregation.service.{AggregationService, OutputQueryService}

trait AggregationProcess
  extends AggregationService
    with UpdateDataset
    with ConfigReader
    with DatasetReader
    with DatasetWriter {

  implicit val logger = Logger("aggregation-service")

  val config = readConfig.unsafeRunSync()

  implicit val sparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate()

  def aggregate(config: AggregationConfig)(implicit sparkSession: SparkSession): Seq[IO[Unit]] = {
    import sparkSession.implicits._

    Seq(
      updateAndOverwrite(config.basePath, aggregateTime, config.violationTimeTable),
      updateAndOverwrite(config.basePath, aggregateReasons,
        config.locationReasonTable, Some(config.reasonTable)),
      updateAndOverwrite(config.basePath, aggregateStates, config.stateTable),
      updateAndOverwrite(config.basePath, aggregateAnnualSummary, config.yearMonthTable),
      updateAndOverwrite(config.basePath, aggregateLocationReason,
        config.locationReasonTable),
      updateAndOverwrite(config.basePath, aggregateLocation, config.yearLocationTable),
      updateAndOverwrite(config.basePath, aggregatePrecinctSquad, config.precinctSquadTable),
      updateAndOverwrite(config.basePath, aggregateCarMake, config.carMakeTable),
      updateAndOverwrite(config.basePath, aggregateCarColor, config.carColorTable)
    )
  }
}
