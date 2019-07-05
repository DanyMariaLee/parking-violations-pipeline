package pv.common.parser

import pv.common.input.InputMetaData
import pv.common.output._
import pv.common.parser.DateTimeParser._
import InputMetaData._
import pv.common.output.domain._

import scala.util.Try

/**
  * Scala has a limit for number of fields = 22,
  * which means we can't use any standard parsers for class containing 51 field:
  * reflection, tuple->apply or even shapeless are helpless here.
  * So we got two options: use flink/spark to read file or write a simple,
  * but very annoying function.
  * I prefer to use simple function, as it's better to reduce dependencies.
  **/
trait CSVParser {

  def fromString(s: String): Option[WrappedNYCData] = {
    implicit val raw = s.split(",")

    Try(raw(RawFieldNames.indexOf(SummonsNumber))).map {
      case sn if sn.nonEmpty =>
        Try {
          val violationData = parseViolation
          val issuerData = parseIssue
          val issueDate = issuerData.date.flatMap(getLocalDate(_, dateFormat1))

          WrappedNYCData(
            sn,
            raw(RawFieldNames.indexOf(PlateId)),
            getOptString(RegistrationState),
            getOptString(PlateType),
            parseVehicle,
            parseAddress,
            violationData,
            issuerData,
            getOptString(TimeFirstObserved),
            getOptString(DateFirstObserved),
            getOptInt(LawSection),
            getOptString(SubDivision),
            getOptString(DaysParkingInEffect),
            hoursInEffect = HoursInEffect(
              getOptString(FromHoursInEffect),
              getOptString(ToHoursInEffect)
            ),
            getOptString(MeterNumber),
            getOptString(FeetFromCurb).flatMap(s => Try(s.toInt).toOption),
            violationDate = issueDate.map(dt => ViolationDate(dt.getYear, dt.getMonthValue)),
            violationTime = violationData.time.flatMap(getHourAndMinutes)
          )
        }.toOption
      case _ => None
    }.toOption.flatten
  }

  private def getOptString(field: String)(implicit rawData: Array[String]): Option[String] =
    rawData.lift(RawFieldNames.indexOf(field))

  private def getOptInt(field: String)(implicit rawData: Array[String]): Option[Int] =
    getOptString(field).flatMap(s => Try(s.toInt).toOption)

  private def parseVehicle(implicit rawData: Array[String]) = Vehicle(
    getOptString(VehicleBodyType),
    getOptString(VehicleMake),
    getOptString(VehicleColor),
    getOptString(UnregisteredVehicle),
    getOptInt(VehicleYear),
    getOptString(VehicleExpirationDate)
  )

  private def parseAddress(implicit rawData: Array[String]) = AddressData(
    getOptInt(StreetCode1),
    getOptInt(StreetCode2),
    getOptInt(StreetCode3),
    getOptString(HouseNumber),
    getOptString(StreetName),
    getOptString(IntersectingStreet)
  )

  private def parseViolation(implicit rawData: Array[String]) = ViolationData(
    getOptInt(ViolationCode),
    getOptString(ViolationLocation),
    getOptInt(ViolationPrecinct),
    getOptString(ViolationTimeField),
    getOptString(ViolationCounty),
    getOptString(ViolationInFrontOfOrOpposite),
    getOptString(ViolationLegalCode),
    getOptString(ViolationPostCode),
    getOptString(ViolationDescription),
    getOptString(NoStandingOrStoppingViolation),
    getOptString(HydrantViolation),
    getOptString(DoubleParkingViolation)
  )

  private def parseIssue(implicit rawData: Array[String]) = IssueData(
    getOptString(IssuingAgency),
    getOptString(IssueDate),
    getOptInt(IssuerPrecinct),
    getOptInt(IssuerCode).map(_.toLong),
    getOptString(IssuerCommand),
    getOptString(IssuerSquad)
  )
}


