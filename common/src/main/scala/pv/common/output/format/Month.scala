package pv.common.output.format

sealed trait Month {
  def number: Int
}

case object January extends Month {
  val number = 1
}

case object February extends Month {
  val number = 2
}

case object March extends Month {
  val number = 3
}

case object April extends Month {
  val number = 4
}

case object May extends Month {
  val number = 5
}

case object June extends Month {
  val number = 6
}

case object July extends Month {
  val number = 7
}

case object August extends Month {
  val number = 8
}

case object September extends Month {
  val number = 9
}

case object October extends Month {
  val number = 10
}

case object November extends Month {
  val number = 11
}

case object December extends Month {
  val number = 12
}
