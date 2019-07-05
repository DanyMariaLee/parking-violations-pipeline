package pv.data.processing.analytics

import org.apache.spark.sql.{Dataset, SparkSession}
import pv.common.output.domain.WrappedNYCData
import pv.common.output.format.Months
import pv.common.output.table._

trait AnalyticsService {


  def stateSummary(ds: Dataset[WrappedNYCData]
                  )(implicit ss: SparkSession): Dataset[AnnualStateSummary] = {
    import ss.implicits._
    ds
      .filter(d => d.registrationState.exists(_.nonEmpty) && d.violationDate.nonEmpty)
      .groupByKey(_.violationDate.map(_.year))
      .flatMapGroups {
        case (Some(year), iter) =>
          iter.toVector.groupBy(_.registrationState).map {
            case (Some(state), data) =>
              AnnualStateSummary(year, state, data.length)
          }
      }
  }

  def annualSummary(ds: Dataset[WrappedNYCData]
                   )(implicit ss: SparkSession): Dataset[AnnualMonthSummary] = {
    import ss.implicits._

    ds
      .filter(d => d.violationDate.nonEmpty)
      .groupByKey(_.violationDate.map(_.year))
      .flatMapGroups {
        case (Some(year), iter) =>
          iter.toVector.groupBy(_.violationDate.map(_.month))
            .flatMap {
              case (Some(month), data) =>
                Months.findName(month).map(monthName =>
                  AnnualMonthSummary(year, monthName, data.length))
            }
      }
  }

  def violationTimeSummary(ds: Dataset[WrappedNYCData]
                          )(implicit ss: SparkSession): Dataset[ViolationTimeSummary] = {
    import ss.implicits._

    ds
      .filter(d => d.violationTime.nonEmpty && d.violationDate.nonEmpty)
      .groupByKey(_.violationDate.map(_.year))
      .flatMapGroups {
        case (Some(year), items) =>
          items.toVector
            .flatMap(_.violationTime)
            .groupBy(_.hour)
            .flatMap {
              case (hour, hourData) =>
                hourData.groupBy(_.minutes).map {
                  case (minute, minutes) =>
                    ViolationTimeSummary(year, hour, minute, minutes.length)
                }
            }
      }
  }

  def locationReasonSummary(ds: Dataset[WrappedNYCData]
                           )(implicit ss: SparkSession): Dataset[LocationReasonSummary] = {
    import ss.implicits._

    ds
      .filter(d =>
        d.violationData.location.exists(_.nonEmpty) &&
          d.violationData.description.exists(_.nonEmpty)
      )
      .groupByKey(_.violationData.location)
      .flatMapGroups {
        case (Some(location), iter) =>
          iter.toVector
            .groupBy(_.violationData.description)
            .map { case (Some(desc), items) =>
              LocationReasonSummary(location, desc, items.length)
            }
      }
  }

  def annualLocationSummary(ds: Dataset[WrappedNYCData]
                           )(implicit ss: SparkSession): Dataset[AnnualLocationSummary] = {
    import ss.implicits._

    ds
      .filter(d =>
        d.violationDate.nonEmpty &&
          d.violationData.location.exists(_.nonEmpty)
      )
      .groupByKey(_.violationDate.map(_.year))
      .flatMapGroups {
        case (Some(year), iter) =>
          iter.toVector.groupBy(_.violationData.location)
            .map { case (Some(location), items) =>
              AnnualLocationSummary(year, location, items.length)
            }
      }
  }

  def precinctSquadSummary(ds: Dataset[WrappedNYCData]
                          )(implicit ss: SparkSession): Dataset[PrecinctSquadSummary] = {
    import ss.implicits._

    ds
      .filter(d => d.issueData.precinct.nonEmpty)
      .groupByKey(_.issueData.precinct)
      .flatMapGroups {
        case (Some(precinct), iter) =>
          iter.toVector
            .groupBy(_.issueData.squad)
            .map {
              case (Some(squad), violations) =>
                PrecinctSquadSummary(precinct, squad, violations.length)
              case (None, violations) =>
                PrecinctSquadSummary(precinct, "", violations.length)
            }
      }
  }

  def carMakeSummary(ds: Dataset[WrappedNYCData]
                    )(implicit ss: SparkSession): Dataset[CarMakeSummary] = {
    import ss.implicits._

    ds
      .filter(_.vehicle.make.exists(_.nonEmpty))
      .groupByKey(_.vehicle.make)
      .mapGroups {
        case (Some(make), iter) =>
          CarMakeSummary(make, iter.length)
      }
  }

  def carColorSummary(ds: Dataset[WrappedNYCData]
                     )(implicit ss: SparkSession): Dataset[CarColorSummary] = {
    import ss.implicits._

    ds
      .filter(_.vehicle.color.exists(_.nonEmpty))
      .map { nycData =>
        val updatedColor = nycData.vehicle.color.map(CarColors.specifyColor)
        nycData.copy(vehicle = nycData.vehicle.copy(color = updatedColor))
      }
      .groupByKey(_.vehicle.color)
      .mapGroups {
        case (Some(color), iter) =>
          CarColorSummary(color, iter.length)
      }
  }
}
