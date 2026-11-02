package uk.gov.nationalarchives.tdr.api.db.repository

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import slick.dbio.{DBIO, DBIOAction, Effect, NoStream}

class BatchInsertSpec extends AnyFlatSpec with Matchers {

  "maxRowsPerStatement" should "return the preferred maximum when it is within the bind parameter limit" in {
    BatchInsert.maxRowsPerStatement(columnsPerRow = 10, preferredMaxRows = 100) should equal(100)
  }

  "maxRowsPerStatement" should "return the default preferred maximum when no preferred maximum is given and the limit allows it" in {
    BatchInsert.maxRowsPerStatement(columnsPerRow = 10) should equal(5000)
  }

  "maxRowsPerStatement" should "cap the number of rows so a statement stays within the bind parameter limit" in {
    val columnsPerRow = 100
    val maxRows = BatchInsert.maxRowsPerStatement(columnsPerRow)
    maxRows should equal(65535 / columnsPerRow)
    maxRows * columnsPerRow should be <= 65535
  }

  "maxRowsPerStatement" should "return at least one row when a single row exceeds the bind parameter limit" in {
    BatchInsert.maxRowsPerStatement(columnsPerRow = 70000) should equal(1)
  }

  "maxRowsPerStatement" should "return at least one row when the preferred maximum is zero or negative" in {
    BatchInsert.maxRowsPerStatement(columnsPerRow = 10, preferredMaxRows = 0) should equal(1)
    BatchInsert.maxRowsPerStatement(columnsPerRow = 10, preferredMaxRows = -5) should equal(1)
  }

  "maxRowsPerStatement" should "treat a non positive column count as one column" in {
    BatchInsert.maxRowsPerStatement(columnsPerRow = 0, preferredMaxRows = 100000) should equal(65535)
    BatchInsert.maxRowsPerStatement(columnsPerRow = -1, preferredMaxRows = 100000) should equal(65535)
  }

  "chunked" should "split the rows into chunks no larger than the maximum rows per statement" in {
    val rows = (1 to 10).toList
    val chunks = capturedChunks(rows, columnsPerRow = 1, preferredMaxRows = 3)

    chunks should equal(List(Seq(1, 2, 3), Seq(4, 5, 6), Seq(7, 8, 9), Seq(10)))
  }

  "chunked" should "return a single chunk when all rows fit in one statement" in {
    val rows = (1 to 10).toList
    val chunks = capturedChunks(rows, columnsPerRow = 5, preferredMaxRows = 10)

    chunks should equal(List(rows))
  }

  "chunked" should "return no actions when there are no rows" in {
    val chunks = capturedChunks(Seq.empty[Int], columnsPerRow = 5, preferredMaxRows = 10)

    chunks should be(empty)
  }

  "chunked" should "chunk by the bind parameter limit when the preferred maximum is larger" in {
    val columnsPerRow = 30000
    val rows = (1 to 5).toList
    val chunks = capturedChunks(rows, columnsPerRow, preferredMaxRows = 5000)

    chunks should equal(List(Seq(1, 2), Seq(3, 4), Seq(5)))
  }

  "chunked" should "return one action per chunk" in {
    val rows = (1 to 7).toList
    val actions = BatchInsert.chunked[Int, Int, Effect](rows, columnsPerRow = 1, preferredMaxRows = 2)(chunk => DBIO.successful(chunk.sum))

    actions.size should equal(4)
  }

  private def capturedChunks(rows: Seq[Int], columnsPerRow: Int, preferredMaxRows: Int): List[Seq[Int]] = {
    var chunks: List[Seq[Int]] = Nil
    val actions: Seq[DBIOAction[Int, NoStream, Effect]] = BatchInsert.chunked[Int, Int, Effect](rows, columnsPerRow, preferredMaxRows) { chunk =>
      chunks = chunks :+ chunk
      DBIO.successful(chunk.size)
    }
    actions.size should equal(chunks.size)
    chunks
  }
}
