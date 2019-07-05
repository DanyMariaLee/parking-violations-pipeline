package pv.aggregation.config

case class AggregationConfig(basePath: String,
                             yearLocationTable: String,
                             locationReasonTable: String,
                             stateTable: String,
                             yearMonthTable: String,
                             violationTimeTable: String,
                             carColorTable: String,
                             carMakeTable: String,
                             reasonTable: String,
                             precinctSquadTable: String,
                             topKResults: Int
                            )