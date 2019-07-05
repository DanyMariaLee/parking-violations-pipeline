package pv.common.parser

import org.scalatest.{FlatSpec, Matchers}
import pv.common.output.domain.ViolationTime

class DateTimeParserSpec extends FlatSpec with Matchers with DateTimeParser {

  behavior of "DateTimeParser"

  "getLocalDate" should "parse date from string" in {

    getLocalDate("08/04/2013", dateFormat1) match {
      case Some(date) =>
        date.getYear shouldBe 2013
        date.getMonthValue shouldBe 8
        date.getDayOfMonth shouldBe 4
      case _ => fail("Failed to parse")
    }

    getLocalDate("20140831", dateFormat2) match {
      case Some(date) =>
        date.getYear shouldBe 2014
        date.getMonthValue shouldBe 8
        date.getDayOfMonth shouldBe 31
      case _ => fail("Failed to parse")
    }
  }

  "getLocalDate" should "not parse date from string" in {

    getLocalDate("zzzzz2013", dateFormat1) match {
      case Some(date) => fail("Should fail to parse")
      case _ =>
    }

    getLocalDate("2014nnn", dateFormat2) match {
      case Some(date) => fail("Should fail to parse")
      case _ =>
    }
  }

  "getHourAndMinutes" should "parse an hour from string" in {

    getHourAndMinutes("0410P") shouldBe Some(ViolationTime(16, 10))
    getHourAndMinutes("0300A") shouldBe Some(ViolationTime(3, 0))
    getHourAndMinutes("1134P") shouldBe Some(ViolationTime(23, 34))
    getHourAndMinutes("1234P") shouldBe Some(ViolationTime(12, 34))
  }

  "getHourAndMinutes" should "not parse an hour from string" in {

    getHourAndMinutes("4414P") shouldBe None
    getHourAndMinutes("1300A") shouldBe None
    getHourAndMinutes("1134") shouldBe None
  }

}
