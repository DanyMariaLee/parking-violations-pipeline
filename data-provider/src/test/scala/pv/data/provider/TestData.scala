package pv.data.provider

import okhttp3._
import pv.common.output._
import pv.common.output.domain._

object TestData {

  val wrappedNYC = WrappedNYCData("1287518126", "WMI", Some("CT"), Some("PAS"),
    Vehicle(Some("SUBN"), Some("ME/BE"), Some("BK"), Some("0"), Some(0), Some("0")),
    AddressData(Some(34510), Some(13610), Some(10610), Some("142"), Some("W 37 ST"), Some("")),
    ViolationData(Some(31), Some("0014"), Some(14), Some("0410P"), Some(""),
      Some("O"), Some(""), None, None, None, None, None),
    IssueData(Some("T"), Some("07/24/2015"), Some(108), Some(330898), Some("T108"), Some("0000")),
    Some(""), Some("0"), Some(408), Some("I3"), Some("YYYYYYB"),
    HoursInEffect(Some("ALL"), Some("ALL")), Some("-"), Some(0), None, None)

  val response200 = new Response.Builder().request(new Request.Builder()
    .url(s"http://localhost:9000/")
    .post(RequestBody.create(MediaType.get("application/json; charset=utf-8"), ""))
    .build())
    .protocol(Protocol.HTTP_1_1)
    .message("")
    .code(200).build()
}
