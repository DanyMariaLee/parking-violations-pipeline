package pv.aggregation.service

import pv.aggregation.TestSparkSessionProvider
import pv.common.output.table._

class AggregationServiceSpec extends TestSparkSessionProvider with AggregationService {

  behavior of "AggregationService"

  "aggregateTime" should "produce summary for each year by hours and minutes" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val input = Seq(
      ViolationTimeSummary(2015, 10, 5, 10),
      ViolationTimeSummary(2015, 10, 10, 10),
      ViolationTimeSummary(2015, 10, 10, 10),
      ViolationTimeSummary(2015, 10, 5, 10),
      ViolationTimeSummary(2015, 10, 25, 10),
      ViolationTimeSummary(2015, 10, 25, 10),
      ViolationTimeSummary(2015, 10, 20, 10),
      ViolationTimeSummary(2015, 10, 20, 10),
      ViolationTimeSummary(2015, 10, 20, 10),
      ViolationTimeSummary(2015, 10, 20, 10),
      ViolationTimeSummary(2015, 10, 5, 10)
    )

    val result = aggregateTime(input.toDS()).collect()

    val expected = Array(
      ViolationTimeSummary(2015, 10, 5, 30),
      ViolationTimeSummary(2015, 10, 10, 20),
      ViolationTimeSummary(2015, 10, 20, 40),
      ViolationTimeSummary(2015, 10, 25, 20)
    )

    result should contain theSameElementsAs expected
  }

  "aggregateReasons" should "produce summary for violation reasons" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val input = Seq(
      LocationReasonSummary("1", "14-No Standing", 10),
      LocationReasonSummary("1", "38-Failure", 10),
      LocationReasonSummary("10", "14-No Standing", 10),
      LocationReasonSummary("100", "21-No Parking", 10),
      LocationReasonSummary("100", "38-Failure", 10),
      LocationReasonSummary("102", "14-No Standing", 10),
      LocationReasonSummary("102", "38-Failure", 10),
      LocationReasonSummary("104", "14-No Standing", 10),
      LocationReasonSummary("105", "14-No Standing", 10),
      LocationReasonSummary("105", "21-No Parking", 10),
      LocationReasonSummary("107", "PHTO SCHOOL ZN SP", 10),
      LocationReasonSummary("108", "38-Failure", 10),
      LocationReasonSummary("109", "14-No Standing", 10),
      LocationReasonSummary("109", "21-No Parking", 10),
      LocationReasonSummary("109", "38-Failure", 10),
      LocationReasonSummary("112", "PHTO SCHOOL ZN SP", 10),
      LocationReasonSummary("113", "38-Failure", 10),
      LocationReasonSummary("114", "14-No Standing", 10),
      LocationReasonSummary("115", "21-No Parking", 10)
    )

    val result = aggregateReasons(input.toDS()).collect()

    val expected = Array(
      ReasonSummary("14-No Standing", 70),
      ReasonSummary("38-Failure", 60),
      ReasonSummary("21-No Parking", 40),
      ReasonSummary("PHTO SCHOOL ZN SP", 20)
    )

    result should contain theSameElementsAs expected
  }

  "aggregateStates" should "produce summary for different states" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val input = Seq(
      AnnualStateSummary(2015, "ME", 10),
      AnnualStateSummary(2015, "LA", 10),
      AnnualStateSummary(2015, "ME", 10),
      AnnualStateSummary(2015, "LA", 10),
      AnnualStateSummary(2015, "ME", 10),
      AnnualStateSummary(2015, "LA", 10),
      AnnualStateSummary(2015, "LA", 10),
      AnnualStateSummary(2015, "NJ", 10),
      AnnualStateSummary(2015, "NJ", 10)
    )

    val result = aggregateStates(input.toDS()).collect()

    val expected = Array(
      AnnualStateSummary(2015, "ME", 30),
      AnnualStateSummary(2015, "LA", 40),
      AnnualStateSummary(2015, "NJ", 20)
    )

    result should contain theSameElementsAs expected
  }

  "aggregateAnnualSummary" should "produce summary for each year by months" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val input = Seq(
      AnnualMonthSummary(2015, "April", 10),
      AnnualMonthSummary(2015, "June", 10),
      AnnualMonthSummary(2015, "July", 10),
      AnnualMonthSummary(2015, "March", 10),
      AnnualMonthSummary(2015, "August", 10),
      AnnualMonthSummary(2015, "September", 10),
      AnnualMonthSummary(2015, "June", 10),
      AnnualMonthSummary(2015, "July", 10),
      AnnualMonthSummary(2015, "March", 10),
      AnnualMonthSummary(2015, "August", 10),
      AnnualMonthSummary(2015, "September", 10),
      AnnualMonthSummary(2015, "August", 10),
      AnnualMonthSummary(2015, "April", 10),
      AnnualMonthSummary(2015, "July", 10),
      AnnualMonthSummary(2015, "August", 10)
    )

    val result = aggregateAnnualSummary(input.toDS()).collect()

    val expected = Array(
      AnnualMonthSummary(2015, "April", 20),
      AnnualMonthSummary(2015, "June", 20),
      AnnualMonthSummary(2015, "July", 30),
      AnnualMonthSummary(2015, "March", 20),
      AnnualMonthSummary(2015, "August", 40),
      AnnualMonthSummary(2015, "September", 20)
    )

    result should contain theSameElementsAs expected
  }

  "aggregateLocationReason" should "produce summary for each location-reason pair" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val input = Seq(
      LocationReasonSummary("1", "14-No Standing", 10),
      LocationReasonSummary("1", "14-No Standing", 10),
      LocationReasonSummary("1", "21-No Parking", 10),
      LocationReasonSummary("10", "14-No Standing", 10),
      LocationReasonSummary("10", "14-No Standing", 10),
      LocationReasonSummary("10", "14-No Standing", 10),
      LocationReasonSummary("10", "21-No Parking", 10),
      LocationReasonSummary("100", "21-No Parking", 10),
      LocationReasonSummary("100", "38-Failure", 10),
      LocationReasonSummary("100", "38-Failure", 10),
      LocationReasonSummary("100", "38-Failure", 10),
      LocationReasonSummary("100", "38-Failure", 10)
    )

    val result = aggregateLocationReason(input.toDS()).collect()

    val expected = Array(
      LocationReasonSummary("1", "14-No Standing", 20),
      LocationReasonSummary("1", "21-No Parking", 10),
      LocationReasonSummary("10", "14-No Standing", 30),
      LocationReasonSummary("10", "21-No Parking", 10),
      LocationReasonSummary("100", "21-No Parking", 10),
      LocationReasonSummary("100", "38-Failure", 40)
    )

    result should contain theSameElementsAs expected
  }

  "aggregateLocation" should "produce annual summary for each location" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val input = Seq(
      AnnualLocationSummary(2015, "67", 10),
      AnnualLocationSummary(2015, "67", 10),
      AnnualLocationSummary(2015, "67", 10),
      AnnualLocationSummary(2015, "67", 10),
      AnnualLocationSummary(2015, "67", 10),
      AnnualLocationSummary(2015, "67", 10),
      AnnualLocationSummary(2015, "113", 10),
      AnnualLocationSummary(2015, "113", 10),
      AnnualLocationSummary(2015, "113", 10),
      AnnualLocationSummary(2015, "113", 10),
      AnnualLocationSummary(2015, "90", 10),
      AnnualLocationSummary(2015, "90", 10),
      AnnualLocationSummary(2015, "90", 10),
      AnnualLocationSummary(2015, "0076", 10)
    )

    val result = aggregateLocation(input.toDS()).collect()

    val expected = Array(
      AnnualLocationSummary(2015, "67", 60),
      AnnualLocationSummary(2015, "113", 40),
      AnnualLocationSummary(2015, "90", 30),
      AnnualLocationSummary(2015, "0076", 10)
    )

    result should contain theSameElementsAs expected
  }

  "aggregatePrecinctSquad" should "produce summary for precincts " +
    "and squads(if the are present)" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val input = Seq(
      PrecinctSquadSummary(0, "J", 10),
      PrecinctSquadSummary(0, "0", 10),
      PrecinctSquadSummary(0, "0", 10),
      PrecinctSquadSummary(0, "0", 10),
      PrecinctSquadSummary(1, "AB", 10),
      PrecinctSquadSummary(1, "AB", 10),
      PrecinctSquadSummary(1, "AB", 10),
      PrecinctSquadSummary(1, "0", 10),
      PrecinctSquadSummary(2, "0000", 10),
      PrecinctSquadSummary(2, "0000", 10),
      PrecinctSquadSummary(2, "0000", 10),
      PrecinctSquadSummary(2, "QQ", 10)
    )

    val result = aggregatePrecinctSquad(input.toDS()).collect()

    val expected = Array(
      PrecinctSquadSummary(0, "J", 10),
      PrecinctSquadSummary(0, "0", 30),
      PrecinctSquadSummary(1, "AB", 30),
      PrecinctSquadSummary(1, "0", 10),
      PrecinctSquadSummary(2, "0000", 30),
      PrecinctSquadSummary(2, "QQ", 10)
    )

    result should contain theSameElementsAs expected
  }

  "aggregateCarMake" should "produce summary for each car make" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val input = Seq(
      CarMakeSummary("ACURA", 10),
      CarMakeSummary("AUDI", 10),
      CarMakeSummary("AUDI", 10),
      CarMakeSummary("AUDI", 10),
      CarMakeSummary("AUDI", 10),
      CarMakeSummary("BMW", 10),
      CarMakeSummary("BMW", 10),
      CarMakeSummary("BMW", 10),
      CarMakeSummary("BMW", 10),
      CarMakeSummary("CADIL", 10),
      CarMakeSummary("CADIL", 10),
      CarMakeSummary("DODGE", 10)
    )

    val result = aggregateCarMake(input.toDS()).collect()

    val expected = Array(
      CarMakeSummary("ACURA", 10),
      CarMakeSummary("AUDI", 40),
      CarMakeSummary("BMW", 40),
      CarMakeSummary("CADIL", 20),
      CarMakeSummary("DODGE", 10)
    )

    result should contain theSameElementsAs expected
  }

  "aggregateCarColor" should "produce summary for each car color" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val input = Seq(
      CarColorSummary("BEIGE", 10),
      CarColorSummary("BEIGE", 10),
      CarColorSummary("BEIGE", 10),
      CarColorSummary("BEIGE", 10),
      CarColorSummary("BLUE", 10),
      CarColorSummary("BLUE", 10),
      CarColorSummary("BLUE", 10),
      CarColorSummary("BROWN", 10),
      CarColorSummary("BROWN", 10),
      CarColorSummary("BROWN", 10),
      CarColorSummary("BROWN", 10),
      CarColorSummary("GREY", 10)
    )

    val result = aggregateCarColor(input.toDS()).collect()

    val expected = Array(
      CarColorSummary("BEIGE", 40),
      CarColorSummary("BLUE", 30),
      CarColorSummary("BROWN", 40),
      CarColorSummary("GREY", 10)
    )

    result should contain theSameElementsAs expected
  }

}