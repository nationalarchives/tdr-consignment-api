package uk.gov.nationalarchives.tdr.api.utils

import com.typesafe.scalalogging.Logger
import net.logstash.logback.argument.StructuredArguments._

import java.util.UUID

class LoggingUtils(logger: Logger) {
  def logFileFormatStatus(fileCheck: String, fileId: UUID, fileCheckStatus: String): Unit =
    logger.info("File check {} for fileId {} completed with status {}",
      value("fileCheck",fileCheck),
      value("fileId", fileId),
      value("fileCheckStatus", fileCheckStatus))
}

object LoggingUtils {
  def apply(logger: Logger): LoggingUtils = new LoggingUtils(logger)
}
