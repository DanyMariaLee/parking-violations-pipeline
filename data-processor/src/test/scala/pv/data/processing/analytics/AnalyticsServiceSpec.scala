package pv.data.processing.analytics

import pv.common.output.domain.{ViolationDate, ViolationTime, WrappedNYCData}
import pv.common.output.table._
import pv.data.processing.TestData._
import pv.data.processing.TestSparkSessionProvider

class AnalyticsServiceSpec
  extends TestSparkSessionProvider
    with AnalyticsService {

  behavior of "AnalyticsService"

  "carColorSummary" should "calculate how many violations happened with each car color" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val input = Seq(
      (1 to 10).map(_ => dummyWrappedNYCData.copy(vehicle = dummyVehicle.copy(color = Some("WHITE")))),
      (1 to 12).map(_ => dummyWrappedNYCData.copy(vehicle = dummyVehicle.copy(color = Some("WH")))),
      (1 to 3).map(_ => dummyWrappedNYCData.copy(vehicle = dummyVehicle.copy(color = Some("WHT")))),
      (1 to 5).map(_ => dummyWrappedNYCData.copy(vehicle = dummyVehicle.copy(color = Some("GRAY")))),
      (1 to 7).map(_ => dummyWrappedNYCData.copy(vehicle = dummyVehicle.copy(color = Some("G/Y"))))
    ).flatten

    val result = carColorSummary(input.toDS()).collect()

    val expected = Seq(
      CarColorSummary("WHITE", 25),
      CarColorSummary("GRAY", 12)
    )

    result should contain theSameElementsAs expected
  }

  "carMakeSummary" should "calculate how many violations happened with each car make" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val input = Seq(
      (1 to 10).map(_ => dummyWrappedNYCData.copy(vehicle = dummyVehicle.copy(make = Some("AUDI")))),
      (1 to 12).map(_ => dummyWrappedNYCData.copy(vehicle = dummyVehicle.copy(make = Some("BMW")))),
      (1 to 3).map(_ => dummyWrappedNYCData.copy(vehicle = dummyVehicle.copy(make = Some("CADIL")))),
      (1 to 5).map(_ => dummyWrappedNYCData.copy(vehicle = dummyVehicle.copy(make = Some("DODGE")))),
      (1 to 7).map(_ => dummyWrappedNYCData.copy(vehicle = dummyVehicle.copy(make = Some("ACURA"))))
    ).flatten

    val result = carMakeSummary(input.toDS()).collect()

    val expected = Seq(
      CarMakeSummary("AUDI", 10),
      CarMakeSummary("BMW", 12),
      CarMakeSummary("CADIL", 3),
      CarMakeSummary("DODGE", 5),
      CarMakeSummary("ACURA", 7)
    )

    result should contain theSameElementsAs expected
  }

  "stateSummary" should "calculate how many violations " +
    "happened in each state every year" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val stateNY = dummyWrappedNYCData
      .copy(registrationState = Some("NY"), violationDate = Some(ViolationDate(2014, 2)))
    val stateNJ = dummyWrappedNYCData
      .copy(registrationState = Some("NJ"), violationDate = Some(ViolationDate(2014, 2)))
    val stateMA = dummyWrappedNYCData
      .copy(registrationState = Some("MA"), violationDate = Some(ViolationDate(2015, 2)))
    val stateAZ = dummyWrappedNYCData
      .copy(registrationState = Some("AZ"), violationDate = Some(ViolationDate(2014, 2)))
    val noState = dummyWrappedNYCData

    val input =
      (1 to 2).map(_ => stateAZ) ++
        (1 to 4).map(_ => stateMA) ++
        (1 to 4).map(_ => stateNY) ++
        (1 to 4).map(_ => noState) :+
        stateNJ

    val result = stateSummary(input.toDS()).collect().sortBy(_.state)

    val expected = Seq(
      AnnualStateSummary(2014, "AZ", 2),
      AnnualStateSummary(2015, "MA", 4),
      AnnualStateSummary(2014, "NJ", 1),
      AnnualStateSummary(2014, "NY", 4)
    )

    result should contain theSameElementsAs expected

    result.map(_.count).sum shouldBe input.length - 4
  }

  "annualSummary" should "calculate how many violations " +
    "happened each year by months" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val monthData2015 = (0 to 12).flatMap {
      case 0 => (1 to 13).map(_ => dummyWrappedNYCData.copy(violationDate = None))
      case 1 => (1 to 3).map(_ => dummyWrappedNYCData.copy(violationDate = Some(ViolationDate(2015, 1))))
      case 4 => (1 to 7).map(_ => dummyWrappedNYCData.copy(violationDate = Some(ViolationDate(2015, 4))))
      case 7 => (1 to 8).map(_ => dummyWrappedNYCData.copy(violationDate = Some(ViolationDate(2015, 7))))
      case m => (1 to 4).map(_ => dummyWrappedNYCData.copy(violationDate = Some(ViolationDate(2015, m))))
    }

    val monthData2013 = (0 to 12).flatMap {
      case 0 => (1 to 2).map(_ => dummyWrappedNYCData.copy(violationDate = None))
      case 1 => (1 to 13).map(_ => dummyWrappedNYCData.copy(violationDate = Some(ViolationDate(2013, 1))))
      case 4 => (1 to 17).map(_ => dummyWrappedNYCData.copy(violationDate = Some(ViolationDate(2013, 4))))
      case 7 => (1 to 2).map(_ => dummyWrappedNYCData.copy(violationDate = Some(ViolationDate(2013, 7))))
      case m => (1 to 7).map(_ => dummyWrappedNYCData.copy(violationDate = Some(ViolationDate(2013, m))))
    }

    val input = monthData2015 ++ monthData2013

    val result = annualSummary(input.toDS()).collect()

    val expected = Array(
      AnnualMonthSummary(2013, "December", 7),
      AnnualMonthSummary(2013, "August", 7),
      AnnualMonthSummary(2013, "April", 17),
      AnnualMonthSummary(2013, "November", 7),
      AnnualMonthSummary(2013, "September", 7),
      AnnualMonthSummary(2013, "May", 7),
      AnnualMonthSummary(2013, "October", 7),
      AnnualMonthSummary(2013, "June", 7),
      AnnualMonthSummary(2013, "January", 13),
      AnnualMonthSummary(2013, "February", 7),
      AnnualMonthSummary(2013, "July", 2),
      AnnualMonthSummary(2013, "March", 7),
      AnnualMonthSummary(2015, "December", 4),
      AnnualMonthSummary(2015, "August", 4),
      AnnualMonthSummary(2015, "April", 7),
      AnnualMonthSummary(2015, "November", 4),
      AnnualMonthSummary(2015, "September", 4),
      AnnualMonthSummary(2015, "May", 4),
      AnnualMonthSummary(2015, "October", 4),
      AnnualMonthSummary(2015, "June", 4),
      AnnualMonthSummary(2015, "January", 3),
      AnnualMonthSummary(2015, "February", 4),
      AnnualMonthSummary(2015, "July", 8),
      AnnualMonthSummary(2015, "March", 4)
    )

    result should contain theSameElementsAs expected
  }

  "violationTimeSummary" should "calculate how many violations " +
    "happen every hour by minutes each year" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val data10_05AM = dummyWrappedNYCData.copy(
      violationDate = Some(ViolationDate(2015, 2)),
      violationTime = Some(ViolationTime(10, 5))
    )

    val data10_10AM = dummyWrappedNYCData.copy(
      violationDate = Some(ViolationDate(2015, 4)),
      violationTime = Some(ViolationTime(10, 10))
    )

    val data10_25AM = dummyWrappedNYCData.copy(
      violationDate = Some(ViolationDate(2015, 5)),
      violationTime = Some(ViolationTime(10, 25))
    )

    val data10_20AM = dummyWrappedNYCData.copy(
      violationDate = Some(ViolationDate(2015, 2)),
      violationTime = Some(ViolationTime(10, 20))
    )

    val input = Seq(
      data10_05AM,
      data10_05AM.copy(violationDate = Some(ViolationDate(2015, 12))),
      data10_05AM.copy(violationDate = Some(ViolationDate(2015, 10))),
      data10_10AM,
      data10_10AM.copy(violationDate = Some(ViolationDate(2015, 11))),
      data10_20AM,
      data10_25AM,
      data10_25AM.copy(violationDate = Some(ViolationDate(2015, 1))),
      data10_25AM.copy(violationDate = Some(ViolationDate(2015, 3))),
      data10_25AM.copy(violationDate = Some(ViolationDate(2013, 11)))
    )

    val result = violationTimeSummary(input.toDS()).collect()

    val expected = Seq(
      ViolationTimeSummary(2013, 10, 25, 1),
      ViolationTimeSummary(2015, 10, 5, 3),
      ViolationTimeSummary(2015, 10, 10, 2),
      ViolationTimeSummary(2015, 10, 25, 3),
      ViolationTimeSummary(2015, 10, 20, 1)
    )

    result should contain theSameElementsAs expected
  }

  "locationReasonSummary" should "calculate violations by locations " +
    "with each violation reason" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val noLocation = dummyWrappedNYCData.copy(
      violationData = dummyViolationData.copy(code = Some(12)))
    val noDescription = dummyWrappedNYCData.copy(
      violationData = dummyViolationData.copy(location = Some("0061"))
    )
    val location0014 = dummyWrappedNYCData.copy(violationData = dummyViolationData.copy(location = Some("0014")))
    val location0061 = dummyWrappedNYCData.copy(violationData = dummyViolationData.copy(location = Some("0061")))
    val location113 = dummyWrappedNYCData.copy(violationData = dummyViolationData.copy(location = Some("113")))
    val location0013 = dummyWrappedNYCData.copy(violationData = dummyViolationData.copy(location = Some("0013")))

    val input =
      (1 to 2).map(_ => location0014.copy(
        violationData = location0014.violationData.copy(description = Some("14-No Standing")))) ++
        (1 to 12).map(_ => location0014.copy(
          violationData = location0014.violationData.copy(description = Some("21-No Parking")))) ++
        (1 to 3).map(_ => location0061.copy(
          violationData = location0061.violationData.copy(description = Some("21-No Parking")))) ++
        (1 to 4).map(_ => location113.copy(
          violationData = location113.violationData.copy(description = Some("21-No Parking")))) ++
        (1 to 16).map(_ => location113.copy(
          violationData = location113.violationData.copy(description = Some("PHTO SCHOOL ZN SP")))) ++
        (1 to 11).map(_ => location113.copy(
          violationData = location113.violationData.copy(description = Some("38-Failure")))) ++
        (1 to 6).map(_ => location0013.copy(
          violationData = location0013.violationData.copy(description = Some("PHTO SCHOOL ZN SP")))) ++
        (1 to 3).map(_ => noLocation) ++
        (1 to 9).map(_ => noDescription)

    val result = locationReasonSummary(input.toDS()).collect()

    val expected = Array(
      LocationReasonSummary("0013", "PHTO SCHOOL ZN SP", 6),
      LocationReasonSummary("113", "38-Failure", 11),
      LocationReasonSummary("113", "PHTO SCHOOL ZN SP", 16),
      LocationReasonSummary("113", "21-No Parking", 4),
      LocationReasonSummary("0014", "14-No Standing", 2),
      LocationReasonSummary("0014", "21-No Parking", 12),
      LocationReasonSummary("0061", "21-No Parking", 3)
    )

    result should contain theSameElementsAs expected

    result.map(_.count).sum shouldBe input.length - 12
  }

  "annualLocationSummary" should "calculate how many violations happen " +
    "in each location every year" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val noLocation = dummyWrappedNYCData.copy(violationDate = Some(ViolationDate(2017, 2)))
    val noYear = dummyWrappedNYCData.copy(
      violationData = dummyViolationData.copy(location = Some("0061")))

    val year2017 = dummyWrappedNYCData.copy(violationDate = Some(ViolationDate(2017, 2)))
    val year2018 = dummyWrappedNYCData.copy(violationDate = Some(ViolationDate(2018, 2)))
    val year2015 = dummyWrappedNYCData.copy(violationDate = Some(ViolationDate(2015, 2)))

    val locations2017 = (1 to 5).flatMap {
      case 1 => (1 to 20).map(_ => year2017.copy(violationData = dummyViolationData.copy(location = Some("1"))))
      case 3 => (1 to 12).map(_ => year2017.copy(violationData = dummyViolationData.copy(location = Some("3"))))
      case 5 => (1 to 8).map(_ => year2017.copy(violationData = dummyViolationData.copy(location = Some("5"))))
      case n => (1 to 32).map(_ => year2017.copy(violationData = dummyViolationData.copy(location = Some(n.toString))))
    }

    val locations2018 = (1 to 5).flatMap {
      case 1 => (1 to 2).map(_ => year2018.copy(violationData = dummyViolationData.copy(location = Some("1"))))
      case 2 => (1 to 4).map(_ => year2018.copy(violationData = dummyViolationData.copy(location = Some("2"))))
      case 4 => (1 to 6).map(_ => year2018.copy(violationData = dummyViolationData.copy(location = Some("4"))))
      case n => (1 to 8).map(_ => year2018.copy(violationData = dummyViolationData.copy(location = Some(n.toString))))
    }

    val locations2015 = (1 to 5).flatMap {
      case 1 => (1 to 8).map(_ => year2015.copy(violationData = dummyViolationData.copy(location = Some("1"))))
      case 3 => (1 to 4).map(_ => year2015.copy(violationData = dummyViolationData.copy(location = Some("3"))))
      case 4 => (1 to 2).map(_ => year2015.copy(violationData = dummyViolationData.copy(location = Some("4"))))
      case n => (1 to 12).map(_ => year2015.copy(violationData = dummyViolationData.copy(location = Some(n.toString))))
    }

    val input = locations2017 ++ locations2018 ++ locations2015 ++
      (1 to 10).map(_ => noLocation) ++
      (1 to 10).map(_ => noYear)

    val result = annualLocationSummary(input.toDS()).collect()

    val expected = Seq(
      AnnualLocationSummary(2017, "4", 32),
      AnnualLocationSummary(2017, "5", 8),
      AnnualLocationSummary(2017, "1", 20),
      AnnualLocationSummary(2017, "2", 32),
      AnnualLocationSummary(2017, "3", 12),
      AnnualLocationSummary(2018, "4", 6),
      AnnualLocationSummary(2018, "5", 8),
      AnnualLocationSummary(2018, "1", 2),
      AnnualLocationSummary(2018, "2", 4),
      AnnualLocationSummary(2018, "3", 8),
      AnnualLocationSummary(2015, "4", 2),
      AnnualLocationSummary(2015, "5", 12),
      AnnualLocationSummary(2015, "1", 8),
      AnnualLocationSummary(2015, "2", 12),
      AnnualLocationSummary(2015, "3", 4)
    )

    result should contain theSameElementsAs expected

    input.length shouldBe result.map(_.count).sum + 20
  }

  "precinctSquadSummary" should "calculate how many violations registered " +
    "by precinct and which sqauds are responsible" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val noSquad = dummyWrappedNYCData.copy(issueData = dummyIssueData.copy(precinct = Some(102)))
    val precinct102 = dummyWrappedNYCData.copy(issueData = dummyIssueData.copy(precinct = Some(102)))
    val precinct84 = dummyWrappedNYCData.copy(issueData = dummyIssueData.copy(precinct = Some(84)))
    val precinct14 = dummyWrappedNYCData.copy(issueData = dummyIssueData.copy(precinct = Some(14)))

    def genSquads(precinct: WrappedNYCData): Seq[WrappedNYCData] = {
      (1 to 5).flatMap {
        case 1 => (1 to 2).map(_ => precinct.copy(issueData = precinct.issueData.copy(squad = Some("1"))))
        case 3 => (1 to 4).map(_ => precinct.copy(issueData = precinct.issueData.copy(squad = Some("3"))))
        case n => (1 to 8).map(_ => precinct.copy(issueData = precinct.issueData.copy(squad = Some(n.toString))))
      }
    }

    val precinct102WithSquads = genSquads(precinct102)
    val precinct84WithSquads = genSquads(precinct84)
    val precinct14WithSquads = genSquads(precinct14)

    val input = precinct102WithSquads ++
      precinct84WithSquads ++
      precinct14WithSquads ++
      (1 to 10).map(_ => noSquad) ++
      (1 to 3).map(_ => dummyWrappedNYCData)

    val result = precinctSquadSummary(input.toDS()).collect()

    val expected = Array(
      PrecinctSquadSummary(102, "4", 8),
      PrecinctSquadSummary(102, "3", 4),
      PrecinctSquadSummary(102, "5", 8),
      PrecinctSquadSummary(102, "", 10),
      PrecinctSquadSummary(102, "1", 2),
      PrecinctSquadSummary(102, "2", 8),
      PrecinctSquadSummary(84, "4", 8),
      PrecinctSquadSummary(84, "3", 4),
      PrecinctSquadSummary(84, "5", 8),
      PrecinctSquadSummary(84, "1", 2),
      PrecinctSquadSummary(84, "2", 8),
      PrecinctSquadSummary(14, "4", 8),
      PrecinctSquadSummary(14, "3", 4),
      PrecinctSquadSummary(14, "5", 8),
      PrecinctSquadSummary(14, "1", 2),
      PrecinctSquadSummary(14, "2", 8)
    )

    result should contain theSameElementsAs expected

    input.length shouldBe result.map(_.count).sum + 3
  }

}
