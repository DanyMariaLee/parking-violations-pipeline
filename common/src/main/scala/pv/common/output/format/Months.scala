package pv.common.output.format

object Months {

  def allValues = List(January, February, March, April, May, June, July,
    August, September, October, November, December)

  def findName(number: Int): Option[String] =
    allValues.find(_.number == number).map(_.toString)

  def findNumber(monthName: String): Option[Int] =
    allValues.find(_.toString.equalsIgnoreCase(monthName))
      .map(_.number)
}
