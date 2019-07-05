package pv.common.output.domain

case class Vehicle(bodyType: Option[String],
                   make: Option[String],
                   color: Option[String],
                   unregistered: Option[String],
                   year: Option[Int],
                   expirationDate: Option[String] //20140831
                  )
