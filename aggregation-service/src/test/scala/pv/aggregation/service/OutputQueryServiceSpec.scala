package pv.aggregation.service

import pv.aggregation.TestSparkSessionProvider
import pv.common.output.query.MaxViolationTime
import pv.common.output.table.{AnnualMonthSummary, ViolationTimeSummary}

class OutputQueryServiceSpec extends TestSparkSessionProvider with OutputQueryService {

  behavior of "OutputQuery"

  "maxNumberOfViolationsPerTimeOfDay" should "calculate when do most violations happen" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val input1 = Seq(
      ViolationTimeSummary(2015, 14, 33, 10),
      ViolationTimeSummary(2015, 7, 39, 45),
      ViolationTimeSummary(2013, 9, 13, 45),
      ViolationTimeSummary(2014, 22, 12, 25)
    )

    val expected = Seq(
      MaxViolationTime(7, 39, 45),
      MaxViolationTime(9, 13, 45)
    )

    val result = maxNumberOfViolationsPerTimeOfDay(input1.toDS())
      .transform { t => t.show()
      t}.collect()

    result should contain theSameElementsAs expected

    val emptyInputResult = maxNumberOfViolationsPerTimeOfDay(
      Seq.empty[ViolationTimeSummary].toDS()
    ).collect()

    emptyInputResult shouldBe empty
  }

  "numberOfViolationPerMonthIn2015" should "filter out 2015 year and " +
    "aggregate data for each month" in {
    implicit val ss = sparkSession
    import ss.implicits._

    val input = Seq(
      AnnualMonthSummary(2015, "May", 20),
      AnnualMonthSummary(2015, "December", 32),
      AnnualMonthSummary(2015, "July", 12),
      AnnualMonthSummary(2014, "March", 10),
      AnnualMonthSummary(2014, "May", 20),
      AnnualMonthSummary(2015, "January", 12),
      AnnualMonthSummary(2015, "March", 10),
      AnnualMonthSummary(2014, "December", 32)
    )

    val expected = Seq(
      AnnualMonthSummary(2015, "January", 12),
      AnnualMonthSummary(2015, "March", 10),
      AnnualMonthSummary(2015, "May", 20),
      AnnualMonthSummary(2015, "July", 12),
      AnnualMonthSummary(2015, "December", 32)
    )

    val result = numberOfViolationPerMonthIn2015(input.toDS()).collect()

    result should contain theSameElementsAs expected
  }

}
