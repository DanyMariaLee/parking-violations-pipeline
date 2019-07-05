package pv.common.parser

import org.scalatest.{FlatSpec, Matchers}
import pv.common.output.domain._

class CSVParserSpec extends FlatSpec with Matchers with CSVParser {

  behavior of "CSVParser"

  "fromString" should "parse data from string" in {
    val input =
      """1287518126,WMI,CT,PAS,07/24/2015,31,SUBN,ME/BE,T,34510,13610,10610,0,0014,14,108,330898,T108,0000,0410P,,,O,142,W 37 ST,,0,408,I3,,YYYYYYB,ALL,ALL,BK,0,0,-,0,,,,,,,,,,,,,
        |1287603671,P752957,99,999,07/20/2015,85,DELV,VOLVO,T,17330,89740,9530,0,0066,66,108,338568,T108,0000,0246A,1134P,K,F,5810,BAY PARKWAY,,20150719,408,C,,BBBBBBB,ALL,ALL,,0,0,-,0,,,,,,,,,,,,,
        |1287603683,P774757,IL,PAS,03/20/2015,85,TRLR,VOLVO,T,17330,89740,9530,0,0066,66,108,338568,T108,0000,0300A,0950P,K,O,5810,BAY PARKWAY,,20150719,408,K5,,BBBBBBB,ALL,ALL,WHITE,0,0,-,0,,,,,,,,,,,,,""".stripMargin

    val result = input.split("\n").flatMap(fromString)

    val expected = Seq(
      WrappedNYCData("1287518126", "WMI", Some("CT"), Some("PAS"),
        Vehicle(Some("SUBN"), Some("ME/BE"), Some("BK"), Some("0"), Some(0), Some("0")),
        AddressData(Some(34510), Some(13610), Some(10610), Some("142"), Some("W 37 ST"), Some("")),
        ViolationData(Some(31), Some("0014"), Some(14), Some("0410P"), Some(""),
          Some("O"), Some(""), None, None, None, None, None),
        IssueData(Some("T"), Some("07/24/2015"), Some(108), Some(330898), Some("T108"), Some("0000")),
        Some(""), Some("0"), Some(408), Some("I3"), Some("YYYYYYB"),
        HoursInEffect(Some("ALL"), Some("ALL")), Some("-"), Some(0),
        Some(ViolationDate(2015, 7)), Some(ViolationTime(16, 10))
      ),

      WrappedNYCData("1287603671", "P752957", Some("99"), Some("999"),
        Vehicle(Some("DELV"), Some("VOLVO"), Some(""), Some("0"), Some(0), Some("0")),
        AddressData(Some(17330), Some(89740), Some(9530), Some("5810"), Some("BAY PARKWAY"), Some("")),
        ViolationData(Some(85), Some("0066"), Some(66), Some("0246A"), Some("K"), Some("F"), Some(""),
          None, None, None, None, None),
        IssueData(Some("T"), Some("07/20/2015"), Some(108), Some(338568), Some("T108"),
          Some("0000")), Some("1134P"), Some("20150719"), Some(408), Some("C"), Some("BBBBBBB"),
        HoursInEffect(Some("ALL"), Some("ALL")), Some("-"), Some(0),
        Some(ViolationDate(2015, 7)), Some(ViolationTime(2, 46))
      ),

      WrappedNYCData("1287603683", "P774757", Some("IL"), Some("PAS"),
        Vehicle(Some("TRLR"), Some("VOLVO"), Some("WHITE"), Some("0"), Some(0), Some("0")),
        AddressData(Some(17330), Some(89740), Some(9530), Some("5810"), Some("BAY PARKWAY"), Some("")),
        ViolationData(Some(85), Some("0066"), Some(66), Some("0300A"), Some("K"), Some("O"),
          Some(""), None, None, None, None, None),
        IssueData(Some("T"), Some("03/20/2015"), Some(108), Some(338568), Some("T108"),
          Some("0000")), Some("0950P"), Some("20150719"), Some(408), Some("K5"), Some("BBBBBBB"),
        HoursInEffect(Some("ALL"), Some("ALL")), Some("-"), Some(0),
        Some(ViolationDate(2015, 3)), Some(ViolationTime(3, 0)))
    )

    result should contain theSameElementsAs expected
  }

}
