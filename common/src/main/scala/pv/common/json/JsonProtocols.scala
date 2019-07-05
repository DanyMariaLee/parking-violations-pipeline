package pv.common.json

import pv.common.output._
import pv.common.output.domain._
import spray.json.DefaultJsonProtocol

trait JsonProtocols extends DefaultJsonProtocol {

  implicit val violationTimeFormat = jsonFormat2(ViolationTime.apply)
  implicit val violationDateFormat = jsonFormat2(ViolationDate.apply)
  implicit val hoursInEffectFormat = jsonFormat2(HoursInEffect.apply)
  implicit val vehicleFormat = jsonFormat6(Vehicle.apply)
  implicit val addressDataFormat = jsonFormat6(AddressData.apply)
  implicit val violationDataFormat = jsonFormat12(ViolationData.apply)
  implicit val issueDataFormat = jsonFormat6(IssueData.apply)
  implicit val nycDataFormat = jsonFormat18(WrappedNYCData.apply)

}
