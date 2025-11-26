package uk.gov.nationalarchives.tdr.api.utils

import org.apache.pekko.actor.ActorSystem
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object RetryUtils {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private lazy val retryActorSystem: ActorSystem = ActorSystem("retry-system")

  def retry[T](
      f: => Future[T],
      retries: Int,
      delay: FiniteDuration,
      isRetryable: Throwable => Boolean
  )(implicit ec: ExecutionContext): Future[T] = {
    f.recoverWith {
      case e if isRetryable(e) && retries > 0 =>
        val jitter = Random.nextInt(100).millis
        val nextDelay = delay + jitter
        logger.warn(s"Deadlock detected, will retry... Retries left: $retries. Delaying for $nextDelay. Error: ${e.getMessage}")
        implicit val system: ActorSystem = retryActorSystem
        org.apache.pekko.pattern.after(nextDelay)(retry(f, retries - 1, delay, isRetryable))
      case e =>
        logger.error(s"Operation failed after all retries: ${e.getMessage}", e)
        Future.failed(e)
    }
  }
}
