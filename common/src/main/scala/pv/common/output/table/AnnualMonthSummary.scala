package pv.common.output.table

case class AnnualMonthSummary(year: Int, month: String, count: Int)

object AnnualMonthSummary {
  val year = "year"
  val month = "month"
  val count = "count"
}