package pv.common.output.table

case class AnnualLocationSummary(year: Int, location: String, count: Long)

object AnnualLocationSummary {
  val year = "year"
  val location = "location"
  val count = "count"
}