package pv.data.processing.config

case class DataProcessingConfig(bootstrapServers: String,
                                topic: String,
                                groupId: String,
                                basePath: String,
                                yearLocationTable: String,
                                locationReasonTable: String,
                                stateTable: String,
                                yearMonthTable: String,
                                violationTimeTable: String,
                                carColorTable: String,
                                carMakeTable: String,
                                precinctSquadTable: String
                               )