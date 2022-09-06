package uk.gov.nationalarchives.tdr.api.utils

import scala.util.matching.Regex
import java.text.Normalizer

object NaturalSorting {
  implicit object ArrayOrdering extends Ordering[Array[String]] { // 4
    val INT: Regex = "([0-9]+)".r
    def compare(a: Array[String], b: Array[String]) : Int = {
      val l = Math.min(a.length, b.length)
      (0 until l).segmentLength(i => a(i) equals b(i)) match {
        case i if i == l => Math.signum(b.length - a.length).toInt
        case i => (a(i), b(i)) match {
          case (INT(c), INT(d)) => Math.signum(c.toInt - d.toInt).toInt
          case (c, d) => c compareTo d
        }
      }
    }
  }

  def natural(str: String): Array[String] = {
    val replacements = Map('\u00df' -> "ss", '\u017f' -> "s", '\u0292' -> "s").withDefault(s => s.toString) // 8
    Normalizer.normalize(Normalizer.normalize(
      str.trim.toLowerCase, // 1.1, 1.2, 3
      Normalizer.Form.NFKC), // 7
      Normalizer.Form.NFD).replaceAll("\\p{InCombiningDiacriticalMarks}", "") // 6
      .replaceAll("^(the|a|an) ", "") // 5
      .flatMap(replacements.apply) // 8
      .split(s"\\s+|(?=[0-9])(?<=[^0-9])|(?=[^0-9])(?<=[0-9])") // 1.3, 2 and 4
  }
}