package pv.common.output.domain

case class AddressData(streetCode1: Option[Int],
                       streetCode2: Option[Int],
                       streetCode3: Option[Int],
                       houseNumber: Option[String],
                       streetName: Option[String],
                       intersectingStreet: Option[String]
                      )
