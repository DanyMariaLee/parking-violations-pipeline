package pv.data.processing

import pv.common.output.domain._

object TestData {

  val dummyViolationData = ViolationData(None, None, None, None, None, None,
    None, None, None, None, None, None)

  val dummyIssueData = IssueData(None, None, None, None, None, None)

  val dummyVehicle = Vehicle(None, None, None, None, None, None)

  val dummyWrappedNYCData =
    WrappedNYCData(
      "0",
      "",
      None,
      None,
      Vehicle(None, None, None, None, None, None),
      AddressData(None, None, None, None, None, None),
      dummyViolationData,
      dummyIssueData,
      None,
      None,
      None,
      None,
      None,
      HoursInEffect(None, None),
      None,
      None,
      None,
      None
    )

}
