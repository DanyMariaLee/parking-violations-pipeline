package pv.data.processing.analytics

object CarColors {

  val allColors = List(
    ColorPair(List("BG", "BEIG"), "BEIGE"),
    ColorPair(List("BK", "BLAC", "BLK"), "BLACK"),
    ColorPair(List("BL", "BLU"), "BLUE"),
    ColorPair(List("BN", "BRWN", "BRW"), "BROWN"),
    ColorPair(List("BZ"), "BRONZE"),
    ColorPair(List("CH"), "CHARCOAL"),
    ColorPair(List("GD", "GLD"), "GOLD"),
    ColorPair(List("GN", "GRN"), "GREEN"),
    ColorPair(List("GT", "GRNT"), "GRANITE"),
    ColorPair(List("GY", "G/Y", "GRY"), "GRAY"),
    ColorPair(List("RD"), "RED"),
    ColorPair(List("TN"), "TAN"),
    ColorPair(List("WT", "WH", "WHT"), "WHITE"),
    ColorPair(List("YL", "YELL", "YELLW"), "YELLOW")
  )

  def specifyColor(s: String): String = allColors.find(s).getOrElse(s)

  implicit class PickColor(ls: List[ColorPair]) {
    def find(s: String) =
      ls.find(cp => cp.shorten.exists(_.equalsIgnoreCase(s)) ||
        cp.full.equalsIgnoreCase(s)).map(_.full)
  }

}

case class ColorPair(shorten: List[String], full: String)
