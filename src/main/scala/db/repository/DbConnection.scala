package db.repository

import akka.stream.alpakka.slick.javadsl.SlickSession

object DbConnection {
  val db = SlickSession.forConfig("consignmentapi").db
}
