package pv.common.output.domain

// Akka marshallers can handle no more than 22 parameters in case class
case class WrappedNYCData(summonsNumber: String,
                          plateID: String,
                          registrationState: Option[String],
                          plateType: Option[String],
                          vehicle: Vehicle,
                          addressData: AddressData,
                          violationData: ViolationData,
                          issueData: IssueData,
                          timeFirstObserved: Option[String],
                          dateFirstObserved: Option[String], //20130719
                          lawSection: Option[Int],
                          subDivision: Option[String],
                          daysParkingInEffect: Option[String],
                          hoursInEffect: HoursInEffect,
                          meterNumber: Option[String],
                          feetFromCurb: Option[Int],
                          violationDate: Option[ViolationDate],
                          violationTime: Option[ViolationTime]
                         )
