package pv.common.output.table

case class PrecinctSquadSummary(precinct: Int, squad: String, count: Long)

object PrecinctSquadSummary {
  val precinct = "precinct"
  val squad = "squad"
  val count = "count"
}