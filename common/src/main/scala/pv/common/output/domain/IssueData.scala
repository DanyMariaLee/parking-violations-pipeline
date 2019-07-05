package pv.common.output.domain

case class IssueData(agency: Option[String],
                     date: Option[String], // 08/04/2013
                     precinct: Option[Int],
                     code: Option[Long],
                     command: Option[String],
                     squad: Option[String]
                    )
