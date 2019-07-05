package pv.common.output.domain

case class ViolationData(code: Option[Int],
                         location: Option[String],
                         precinct: Option[Int],
                         time: Option[String],
                         county: Option[String],
                         inFrontOfOrOpposite: Option[String],
                         legalCode: Option[String],
                         postCode: Option[String],
                         description: Option[String],
                         noStandingOrStoppingViolation: Option[String],
                         hydrantViolation: Option[String],
                         doubleParkingViolation: Option[String]
                        )
