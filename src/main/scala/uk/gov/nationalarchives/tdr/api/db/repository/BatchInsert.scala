package uk.gov.nationalarchives.tdr.api.db.repository

import slick.dbio.{DBIOAction, Effect, NoStream}

/** Helpers for multi-row inserts using `insertAll(rows, RowsPerStatement.All)`.
  *
  * Slick's `++=` is `insertAll(rows, RowsPerStatement.One)`. When combined with `returning`, Slick disables JDBC batching and runs one `INSERT ... RETURNING` per row. Multi-row
  * statements avoid this, but PostgreSQL limits a single statement to 65535 bind parameters, so rows must be chunked.
  */
object BatchInsert {
  val postgresMaxBindParameters: Int = 65535
  val defaultMaxRowsPerStatement: Int = 5000

  def maxRowsPerStatement(columnsPerRow: Int, preferredMaxRows: Int = defaultMaxRowsPerStatement): Int =
    math.max(1, math.min(preferredMaxRows, postgresMaxBindParameters / math.max(1, columnsPerRow)))

  /** Splits rows into statements that each stay within the PostgreSQL bind-parameter limit. Callers should run the result `transactionally` so all chunks succeed or fail together.
    */
  def chunked[U, R, E <: Effect](rows: Seq[U], columnsPerRow: Int, preferredMaxRows: Int = defaultMaxRowsPerStatement)(
      insertChunk: Seq[U] => DBIOAction[R, NoStream, E]
  ): Seq[DBIOAction[R, NoStream, E]] =
    rows.grouped(maxRowsPerStatement(columnsPerRow, preferredMaxRows)).map(insertChunk).toSeq
}
