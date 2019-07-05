package pv.common.parser

import java.time.LocalDate

import pv.common.output.domain.ViolationTime

import scala.util.{Failure, Success, Try}

trait DateTimeParser {

  val dateFormat1 = "MM/dd/yyyy" //`Issue Date` 08/04/2013
  val dateFormat2 = "yyyyMMdd" //`Vehicle Expiration Date` 20140831

  def getLocalDate(dateString: String, dateFormat: String): Option[LocalDate] = Try {
    val dtf = java.time.format.DateTimeFormatter.ofPattern(dateFormat)
    java.time.LocalDate.parse(dateString, dtf)
  }.toOption

  //0410P, 0300A, 1134P
  def getHourAndMinutes(timeString: String): Option[ViolationTime] =
    Try(timeString.takeRight(1).toUpperCase)
      .map {
        case "A" => fetchViolationTime(timeString)
        case "P" => fetchViolationTime(timeString, true)
        case _ => None
      }
      .toOption.flatten

  private def fetchViolationTime(timeString: String, isNight: Boolean = false): Option[ViolationTime] =
    Try(timeString.take(2).toInt) match {
      case Success(hour) if hour <= 12 =>
        Try(timeString.slice(2, 4).toInt).toOption.map(minutes =>
          ViolationTime(
            if (isNight && hour < 12) hour + 12 else hour,
            minutes
          )
        )
      case _ => None
    }
}

object DateTimeParser extends DateTimeParser
