package pv.common.output.table

case class ViolationTimeSummary(year: Int, hour: Int, minute: Int, count: Long)

object ViolationTimeSummary {
  val year = "year"
  val hour = "hour"
  val minute = "minute"
  val count = "count"
}
