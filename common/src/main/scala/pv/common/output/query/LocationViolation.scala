package pv.common.output.query

case class LocationViolation(location: String, count: Long)

object LocationViolation {
  val location = "location"
  val count = "count"
}