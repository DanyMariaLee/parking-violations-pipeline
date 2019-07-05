package pv.aggregation.service

import org.apache.spark.sql.{Dataset, SparkSession}
import pv.common.output.table._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

/**
  * This service basically squashes the data produces by spark streaming.
  * The reason being - spark doesn't update files, it only appends...
  **/
trait AggregationService {

  def aggregateTime(ds: Dataset[ViolationTimeSummary]
                   )(implicit ss: SparkSession): Dataset[ViolationTimeSummary] = {
    import ss.implicits._
    import ViolationTimeSummary._

    ds
      .groupBy(year, hour, minute)
      .agg(sum(count).as(count))
      .as[ViolationTimeSummary]
  }


  def aggregateReasons(ds: Dataset[LocationReasonSummary]
                      )(implicit ss: SparkSession): Dataset[ReasonSummary] = {
    import ss.implicits._
    ds
      .groupByKey(_.reason)
      .mapGroups {
        case (reason, iter) =>
          ReasonSummary(reason, iter.toVector.map(_.count).sum)
      }
  }

  def aggregateStates(ds: Dataset[AnnualStateSummary]
                     )(implicit ss: SparkSession): Dataset[AnnualStateSummary] = {
    import ss.implicits._
    import AnnualStateSummary._

    ds
      .groupBy(year, state)
      .agg(sum(count).as(count))
      .as[AnnualStateSummary]
  }

  def aggregateAnnualSummary(ds: Dataset[AnnualMonthSummary]
                            )(implicit ss: SparkSession): Dataset[AnnualMonthSummary] = {
    import ss.implicits._
    import AnnualMonthSummary._

    ds
      .groupBy(year, month)
      .agg(sum(count).cast(DataTypes.IntegerType).as(count))
      .as[AnnualMonthSummary]
  }

  def aggregateLocationReason(ds: Dataset[LocationReasonSummary]
                             )(implicit ss: SparkSession): Dataset[LocationReasonSummary] = {
    import ss.implicits._
    import LocationReasonSummary._

    ds.
      groupBy(location, reason)
      .agg(sum(count).as(count))
      .as[LocationReasonSummary]
  }

  def aggregateLocation(ds: Dataset[AnnualLocationSummary]
                       )(implicit ss: SparkSession): Dataset[AnnualLocationSummary] = {
    import ss.implicits._
    import AnnualLocationSummary._

    ds
      .groupBy(year, location)
      .agg(sum(count).as(count))
      .as[AnnualLocationSummary]
  }

  def aggregatePrecinctSquad(ds: Dataset[PrecinctSquadSummary]
                            )(implicit ss: SparkSession): Dataset[PrecinctSquadSummary] = {
    import ss.implicits._
    import PrecinctSquadSummary._

    ds
      .groupBy(precinct, squad)
      .agg(sum(count).as(count))
      .as[PrecinctSquadSummary]
  }

  def aggregateCarMake(ds: Dataset[CarMakeSummary]
                      )(implicit ss: SparkSession): Dataset[CarMakeSummary] = {
    import ss.implicits._

    ds.groupByKey(_.car_make)
      .mapGroups {
        case (make, iter) =>
          CarMakeSummary(make, iter.toVector.map(_.count).sum)
      }
  }

  def aggregateCarColor(ds: Dataset[CarColorSummary]
                       )(implicit ss: SparkSession): Dataset[CarColorSummary] = {
    import ss.implicits._

    ds.groupByKey(_.car_color)
      .mapGroups {
        case (color, iter) =>
          CarColorSummary(color, iter.toVector.map(_.count).sum)
      }
  }

}
