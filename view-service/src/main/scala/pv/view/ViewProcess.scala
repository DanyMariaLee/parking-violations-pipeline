package pv.view

import org.apache.spark.sql.SparkSession
import pv.common.output.query.{LocationViolation, MaxViolationTime, StateSummary}
import pv.common.output.table._
import pv.view.config.ViewConfig
import pv.view.repository.SelectQueryResult
import pv.view.service.{QueryService, ViewService}

trait ViewProcess extends ViewService
  with QueryService
  with SelectQueryResult {

  def buildView(config: ViewConfig)(implicit ss: SparkSession): String = {
    import ss.implicits._

    val data1 = selectResult(config.basePath, config.violationTimeTable, maxNumberOfViolationsPerTimeOfDay)
      .map(_.productIterator.toSeq)

    val data2 = selectResult(config.basePath, config.precinctSquadTable, precinctSquadSorted)
      .map(_.productIterator.toSeq)

    val data3 = selectResult(config.basePath, config.locationReasonTable, reasonByLocationSorted)
      .map(_.productIterator.toSeq)

    val data4 = selectResult(config.basePath, config.yearMonthTable, numberOfViolationPerMonthIn2015)
      .map(_.productIterator.toSeq)

    val data5 = selectTopKResult(
      config.basePath,
      config.stateTable,
      topKRegistrationStatesByViolations,
      config.topKResults
    )
      .map(_.productIterator.toSeq)

    val data6 = selectTopKResult(
      config.basePath,
      config.reasonTable,
      topKCommonReasons,
      config.topKResults
    )
      .map(_.productIterator.toSeq)

    val data7 = selectTopKResult(config.basePath, config.carColorTable, topKCarColors, config.topKResults)
      .map(_.productIterator.toSeq)

    val data8 = selectTopKResult(config.basePath, config.carMakeTable, topKCarMakes, config.topKResults)
      .map(_.productIterator.toSeq)

    val data9 = selectTopKResult(
      config.basePath,
      config.yearLocationTable,
      topKLocationsWithMostViolations,
      config.topKResults
    )
      .map(_.productIterator.toSeq)

    val p1 = page[MaxViolationTime](data1)
    val p2 = page[PrecinctSquadSummary](data2)
    val p3 = page[LocationReasonSummary](data3)
    val p4 = page[AnnualMonthSummary](data4)
    val p5 = page[StateSummary](data5, Some(config.topKResults))
    val p6 = page[ReasonSummary](data6, Some(config.topKResults))
    val p7 = page[CarColorSummary](data7, Some(config.topKResults))
    val p8 = page[CarMakeSummary](data8, Some(config.topKResults))
    val p9 = page[LocationViolation](data9, Some(config.topKResults))

    List(p1, p2, p3, p4, p5, p6, p7, p8, p9).mkString("\n")
  }
}
