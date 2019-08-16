package pv.view.table

import pv.common.output.query.{LocationViolation, MaxViolationTime, StateSummary}
import pv.common.output.table.{AnnualMonthSummary, CarColorSummary, CarMakeSummary, LocationReasonSummary, PrecinctSquadSummary, ReasonSummary}

sealed trait QueryMeta[T] {

  def columnNames: Seq[String]
  protected def question: String
  def getQuestion(topK: Option[Int]) =
    topK.map(value => s"$question (Top-$value)").getOrElse(question)

}

object QueryMeta {

  implicit case object maxViolationTime extends QueryMeta[MaxViolationTime] {
    val columnNames = Seq("hour", "minute", "violations")
    val question = "Number of violations per time of day (when do most violations happen)?"
  }

  implicit case object precinctSquadSummary extends QueryMeta[PrecinctSquadSummary] {
    val columnNames = Seq("precinct", "squad", "count")
    val question = "Number violations issued per police officer or precinct?"
  }

  implicit case object locationReasonSummary extends QueryMeta[LocationReasonSummary] {
    val columnNames = Seq("location", "reason", "count")
    val question = "Reason of violation per location?"
  }

  implicit case object annualMonthSummary extends QueryMeta[AnnualMonthSummary] {
    val columnNames = Seq("year", "month", "count")
    val question = "Number of violation per month in 2015?"
  }

  implicit case object stateSummary extends QueryMeta[StateSummary] {
    val columnNames = Seq("state", "count")
    val question = "Most common registration state among all violations"
  }

  implicit case object reasonSummary extends QueryMeta[ReasonSummary] {
    val columnNames = Seq("description", "count")
    val question = "Most common reasons of all violation"
  }

  implicit case object carColorSummary extends QueryMeta[CarColorSummary] {
    val columnNames = Seq("car_color", "count")
    val question = "Most common car color among all violations"
  }

  implicit case object carMakeSummary extends QueryMeta[CarMakeSummary] {
    val columnNames = Seq("car_make", "count")
    val question = "Most common car makes among all violations"
  }

  implicit case object locationViolation extends QueryMeta[LocationViolation] {
    val columnNames = Seq("location", "count")
    val question = "Locations with most violations"
  }

}