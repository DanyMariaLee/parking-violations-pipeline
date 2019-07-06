package pv.aggregation.service

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import pv.common.output.format.Months
import pv.common.output.query.{LocationViolation, MaxViolationTime, StateSummary}
import pv.common.output.table._

/**
  * The goal of this service is to answer the questions in the task,
  * it's for demo purposes only as in real life
  * we can select such things using spark shell for example.
  **/
trait OutputQueryService {

  /** Number of violation per month in 2015? */
  def numberOfViolationPerMonthIn2015(ds: Dataset[AnnualMonthSummary]
                                     )(implicit ss: SparkSession): Dataset[AnnualMonthSummary] = {
    import ss.implicits._

    val monthNumberCol = "month_number"

    ds
      .filter(_.year == 2015) // 12 rows max
      .flatMap { summary =>
      Months.findNumber(summary.month)
        .map(monthNumber =>
          AnnualMonthSummaryWithMonth(
            monthNumber, summary.year, summary.month, summary.count)
        )
    }
      .sort(monthNumberCol)
      .drop(monthNumberCol)
      .as[AnnualMonthSummary]
  }

  /** Number of violations per time of day (when do most violations happen)? */
  def maxNumberOfViolationsPerTimeOfDay(ds: Dataset[ViolationTimeSummary]
                                       )(implicit ss: SparkSession): Dataset[MaxViolationTime] = {
    import ViolationTimeSummary.count
    import ss.implicits._

    ds.sort(desc(count))
      .limit(1).collect() // limit creates new dataset and does't trigger query execution
      .headOption
      .map(_.count)
      .map { maxCount =>
        ds.filter(_.count == maxCount)
          .map(v => MaxViolationTime(v.hour, v.minute, maxCount))
      }.getOrElse(
      Seq.empty[MaxViolationTime].toDS()
    ).as[MaxViolationTime]
  }

  /** Reason of violation per location? */
  def reasonByLocationSorted(ds: Dataset[LocationReasonSummary]
                            )(implicit ss: SparkSession): Dataset[LocationReasonSummary] =
    ds.sort(desc(LocationReasonSummary.count))

  /** Number violations issued per police officer or precinct? */
  def precinctSquadSorted(ds: Dataset[PrecinctSquadSummary]
                         )(implicit ss: SparkSession): Dataset[PrecinctSquadSummary] =
    ds.sort(desc(PrecinctSquadSummary.count))

  /** Most common reasons of all violation (Top-k) */
  def topKCommonReasons(ds: Dataset[ReasonSummary], topK: Int
                       )(implicit ss: SparkSession): Dataset[ReasonSummary] = {
    ds.sort(desc(ReasonSummary.count)).limit(topK)
  }

  /** Most common car color among all violations (Top-k) */
  def topKCarColors(ds: Dataset[CarColorSummary], topK: Int
                   )(implicit ss: SparkSession): Dataset[CarColorSummary] = {
    ds.sort(desc(CarColorSummary.count)).limit(topK)
  }

  /** Most common car make among all violations (Top-k) */
  def topKCarMakes(ds: Dataset[CarMakeSummary], topK: Int
                  )(implicit ss: SparkSession): Dataset[CarMakeSummary] = {
    ds.sort(desc(CarMakeSummary.count)).limit(topK)
  }

  /** Most common registration state among all violations (Top-k) */
  def topKRegistrationStatesByViolations(ds: Dataset[AnnualStateSummary], topK: Int
                                        )(implicit ss: SparkSession): Dataset[StateSummary] = {
    import StateSummary._
    import ss.implicits._

    ds
      .map(s => StateSummary(s.state, s.count))
      .groupBy(state)
      .agg(sum(count).as(count))
      .as[StateSummary]
      .sort(desc(count))
      .limit(topK)
  }

  /** Locations with most violations (Top-K) */
  def topKLocationsWithMostViolations(ds: Dataset[AnnualLocationSummary], topK: Int
                                     )(implicit ss: SparkSession): Dataset[LocationViolation] = {
    import LocationViolation._
    import ss.implicits._

    ds
      .map(s => LocationViolation(s.location, s.count))
      .groupBy(location)
      .agg(sum(count).as(count))
      .as[LocationViolation]
      .sort(desc(count))
      .limit(topK)
  }
}

case class AnnualMonthSummaryWithMonth(month_number: Int, year: Int, month: String, count: Int)