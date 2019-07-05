package pv.common.output.table

case class AnnualStateSummary(year: Int, state: String, count: Long)

object AnnualStateSummary {
  val year = "year"
  val state = "state"
  val count = "count"
}