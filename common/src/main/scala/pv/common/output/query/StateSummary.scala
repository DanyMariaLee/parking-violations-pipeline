package pv.common.output.query

case class StateSummary(state: String, count: Long)

object StateSummary {
  val state = "state"
  val count = "count"
}
