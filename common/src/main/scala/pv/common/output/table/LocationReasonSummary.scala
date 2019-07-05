package pv.common.output.table

case class LocationReasonSummary(location: String, reason: String, count: Long)

object LocationReasonSummary {
  val location = "location"
  val reason = "reason"
  val count = "count"
}