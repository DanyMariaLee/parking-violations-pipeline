package pv.view.service

import pv.view.table.QueryMeta
import scala.reflect.ClassTag

trait ViewService {

  def page[T](table: Seq[Seq[Any]], topK: Option[Int] = None)(implicit t: ClassTag[T], cn: QueryMeta[T]): String = {
    val data = formatTable(cn.columnNames +: table)
    s"""<body style="color:white;background-color:black;">
       |<h1 style="color:lightgreen;">
       |${cn.getQuestion(topK)}\n</h1>""".stripMargin +
      s"<PRE>$data</PRE>"
  }

  def formatTable(table: Seq[Seq[Any]]): String = {
    if (table.isEmpty) ""
    else {
      // Get column widths based on the maximum cell width in each column (+2 for a one character padding on each side)
      val colWidths = table.transpose.map(_.map(cell => if (cell == null) 0 else cell.toString.length).max + 2)

      def formatRow(item: Any, size: Int) = (" %-" + (size - 1) + "s").format(item)

      // Format each row
      val rows = table.map(
        _.zip(colWidths)
          .map {
            case (item, size) if item == None || item == null => formatRow("", size)
            case (item, size) => formatRow(item, size)
          }
          .mkString("|", "|", "|")
      )
      // Formatted separator row, used to separate the header and draw table borders
      val separator = colWidths.map("-" * _).mkString("+", "+", "+")
      // Put the table together and return
      (separator +: rows.head +: separator +: rows.tail :+ separator).mkString("\n")
    }
  }
}
