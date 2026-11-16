package uk.gov.nationalarchives.tdr.api.manual

import io.circe.Json
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ssm.SsmClient
import software.amazon.awssdk.services.ssm.model.GetParameterRequest
import sttp.client3.{HttpURLConnectionBackend, Identity, RequestT, SttpBackend, UriContext, basicRequest}
import uk.gov.nationalarchives.tdr.keycloak.{KeycloakUtils, TdrKeycloakDeployment}

import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Manual harness for investigating the `healthcheck-full` timeouts seen on staging while clients poll the `fileChecks` progress of a consignment.
  *
  * It applies load to the file checks progress query against a deployed environment and, at the same time, polls `/healthcheck` and `/healthcheck-full` so that you can see whether
  * and for how long the health check endpoints stop responding.
  *
  * A single query is cheap, so to reproduce the outage you need concurrency and duration rather than a big consignment. Every GraphQL request and every `/healthcheck-full` request
  * builds a new `DbConnection`, which resolves AWS credentials and generates an RDS IAM token on the request thread (the memoisation cache in `DbConnectionBase` is a per instance
  * `val`, so it never gets a hit), so the endpoints degrade as concurrent request volume goes up. Start with something like:
  * {{{
  * TDR_CONCURRENT_QUERIES=50 TDR_QUERY_REPEAT_SECONDS=120 sbt "testOnly *FileChecksProgressHealthCheck"
  * }}}
  *
  * This class is deliberately not named `*Spec` so that the CI test matrix (which runs one job per `*Spec.scala` file) does not pick it up, and it cancels itself unless
  * `TDR_CONSIGNMENT_ID` is set.
  *
  * Run it with:
  * {{{
  * export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_SESSION_TOKEN=...   # staging account credentials
  * export TDR_CONSIGNMENT_ID=<a staging consignment>
  * sbt "testOnly *FileChecksProgressHealthCheck"
  * }}}
  */
class FileChecksProgressHealthCheck extends AnyFlatSpec with Matchers {

  private val environment = env("TDR_ENVIRONMENT", "staging")
  private val apiBaseUrl = env("TDR_API_URL", s"https://api.tdr-$environment.nationalarchives.gov.uk")
  private val authUrl = env("TDR_AUTH_URL", s"https://auth.tdr-$environment.nationalarchives.gov.uk")
  private val clientId = env("TDR_CLIENT_ID", "tdr-backend-checks")
  private val clientSecretPath = env("TDR_CLIENT_SECRET_PATH", s"/$environment/keycloak/backend_checks_client/secret")
  private val awsRegion = env("AWS_REGION", "eu-west-2")
  private val queryType = env("TDR_QUERY", "summary")
  private val concurrentQueries = env("TDR_CONCURRENT_QUERIES", "1").toInt
  private val repeatFor = env("TDR_QUERY_REPEAT_SECONDS", "0").toInt.seconds
  private val pollInterval = env("TDR_HEALTHCHECK_INTERVAL_SECONDS", "1").toInt.seconds
  private val healthCheckTimeout = env("TDR_HEALTHCHECK_TIMEOUT_SECONDS", "10").toInt.seconds
  private val queryTimeout = env("TDR_QUERY_TIMEOUT_SECONDS", "300").toInt.seconds
  private val coolDown = env("TDR_COOL_DOWN_SECONDS", "15").toInt.seconds
  private val slowQueryMs = env("TDR_SLOW_QUERY_MILLIS", "1000").toInt
  private val baselineSamples = 3
  private val tokenTtlSeconds = 3600
  private val bodySnippetLength = 800

  private implicit val sttpBackend: SttpBackend[Identity, Any] = HttpURLConnectionBackend()
  private implicit val keycloakDeployment: TdrKeycloakDeployment = TdrKeycloakDeployment(authUrl, "tdr", tokenTtlSeconds)

  private val healthCheckPaths = List("healthcheck", "healthcheck-full")
  private val okOutcome = "HTTP 200"

  private val progressSummaryQuery =
    """query getFileCheckProgressSummary($consignmentId: UUID!) {
      |  getConsignment(consignmentid: $consignmentId) {
      |    allChecksSucceeded
      |    totalFiles
      |    fileChecks {
      |      antivirusProgress { filesProcessed }
      |      checksumProgress { filesProcessed }
      |      ffidProgress { filesProcessed }
      |    }
      |    consignmentStatuses { statusType value }
      |  }
      |}""".stripMargin

  private val progressFullQuery =
    """query getFileCheckProgress($consignmentId: UUID!) {
      |  getConsignment(consignmentid: $consignmentId) {
      |    allChecksSucceeded
      |    parentFolder
      |    consignmentReference
      |    totalFiles
      |    files {
      |      fileStatuses { fileId statusType statusValue }
      |    }
      |    fileChecks {
      |      antivirusProgress { filesProcessed }
      |      checksumProgress { filesProcessed }
      |      ffidProgress { filesProcessed }
      |    }
      |    consignmentStatuses { statusType value }
      |  }
      |}""".stripMargin

  "the consignment API health check" should "keep responding while the file checks progress query runs" in {
    val consignmentId = sys.env
      .get("TDR_CONSIGNMENT_ID")
      .map(id => UUID.fromString(id.trim))
      .getOrElse(cancel("Set TDR_CONSIGNMENT_ID (and AWS credentials for the environment) to run this manual check"))

    val query = queryType match {
      case "summary" => progressSummaryQuery
      case "full"    => progressFullQuery
      case other     => fail(s"TDR_QUERY must be either 'summary' or 'full' but was '$other'")
    }

    val timeline = new Timeline
    timeline.printHeader(consignmentId)

    val accessToken = sys.env.get("TDR_ACCESS_TOKEN").map(_.trim).getOrElse(serviceAccountToken())
    println(s"Authenticated as client '$clientId'\n")

    println("--- Baseline health checks (no load) ---")
    val baseline = (1 to baselineSamples).toList.flatMap(_ => sampleHealthChecks(timeline))

    println(s"\n--- Load: $concurrentQueries concurrent '$queryType' ${if (concurrentQueries == 1) "client" else "clients"} for ${repeatFor.toSeconds}s ---")
    println(s"    (only the first, slow (>${slowQueryMs}ms) and non-200 queries are logged live)")
    val request = progressRequest(query, consignmentId, accessToken)
    val keepPolling = new AtomicBoolean(true)
    val executor = Executors.newFixedThreadPool(concurrentQueries + 1)
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(executor)

    val (queryEvents, loadEvents) =
      try {
        val poller = Future(pollHealthChecks(timeline, keepPolling))
        val loadStartMs = timeline.elapsedMs
        val queries = Future.sequence((1 to concurrentQueries).toList.map(worker => Future(runProgressQueries(timeline, request, worker, loadStartMs))))
        val completedQueries = Await.result(queries, repeatFor + queryTimeout + 1.minute).flatten
        println(s"\n--- Load finished, continuing to poll for ${coolDown.toSeconds} seconds ---")
        Thread.sleep(coolDown.toMillis)
        keepPolling.set(false)
        val completedPolls = Await.result(poller, pollInterval + healthCheckTimeout * 2 + 10.seconds)
        (completedQueries, completedPolls)
      } finally {
        keepPolling.set(false)
        executor.shutdownNow()
      }

    printSummary("File checks progress query", queryEvents)
    printSummary("Health checks, baseline (no load)", baseline)
    printSummary("Health checks, during load and cool down", loadEvents)

    val unhealthy = loadEvents.filterNot(_.outcome == okOutcome)
    withClue(s"Health checks that did not return $okOutcome while the file checks progress query was under load:\n${unhealthy.map(_.describe).mkString("\n")}\n") {
      unhealthy shouldBe empty
    }
  }

  private def progressRequest(query: String, consignmentId: UUID, accessToken: String): RequestT[Identity, Either[String, String], Any] = {
    val requestBody = Json
      .obj(
        "query" -> Json.fromString(query),
        "variables" -> Json.obj("consignmentId" -> Json.fromString(consignmentId.toString))
      )
      .noSpaces

    basicRequest
      .post(uri"$apiBaseUrl/graphql")
      .contentType("application/json")
      .auth
      .bearer(accessToken)
      .body(requestBody)
      .readTimeout(queryTimeout)
  }

  private def runProgressQueries(timeline: Timeline, request: RequestT[Identity, Either[String, String], Any], worker: Int, loadStartMs: Long): List[Event] = {
    val events = ListBuffer[Event]()
    var keepGoing = true
    while (keepGoing) {
      val startedAt = timeline.elapsedMs
      val (outcome, body, durationMs) = send(request)
      val interesting = events.isEmpty || outcome != okOutcome || durationMs > slowQueryMs
      val detail = if (interesting) summariseBody(body) else ""
      events += timeline.record(s"POST /graphql ($worker)", outcome, detail, durationMs, startedAt, interesting)
      keepGoing = timeline.elapsedMs - loadStartMs < repeatFor.toMillis
    }
    events.toList
  }

  private def pollHealthChecks(timeline: Timeline, keepPolling: AtomicBoolean): List[Event] = {
    val samples = ListBuffer[Event]()
    while (keepPolling.get()) {
      samples ++= sampleHealthChecks(timeline)
      if (keepPolling.get()) Thread.sleep(pollInterval.toMillis)
    }
    samples.toList
  }

  private def sampleHealthChecks(timeline: Timeline): List[Event] = healthCheckPaths.map { path =>
    val request = basicRequest.get(uri"$apiBaseUrl/$path").readTimeout(healthCheckTimeout)
    val startedAt = timeline.elapsedMs
    val (outcome, body, durationMs) = send(request)
    timeline.record(s"GET /$path", outcome, if (outcome == okOutcome) "" else summariseBody(body), durationMs, startedAt, print = true)
  }

  private def send(request: RequestT[Identity, Either[String, String], Any]): (String, String, Long) = {
    val startNanos = System.nanoTime()
    val result = Try(request.send(sttpBackend))
    val durationMs = (System.nanoTime() - startNanos) / 1000000
    result match {
      case Success(response) => (s"HTTP ${response.code.code}", response.body.fold(identity, identity), durationMs)
      case Failure(error)    => (s"${error.getClass.getSimpleName}: ${Option(error.getMessage).getOrElse("")}", "", durationMs)
    }
  }

  private def summariseBody(body: String): String = if (body.isEmpty) "" else body.replaceAll("\\s+", " ").take(bodySnippetLength)

  private def serviceAccountToken(): String = {
    implicit val tokenExecutionContext: ExecutionContext = ExecutionContext.global
    val clientSecret = sys.env.get("TDR_CLIENT_SECRET").map(_.trim).getOrElse(ssmParameter(clientSecretPath))
    Await.result(KeycloakUtils().serviceAccountToken(clientId, clientSecret), 1.minute).getValue
  }

  private def ssmParameter(name: String): String = {
    println(s"Reading the client secret from the '$name' SSM parameter in $awsRegion")
    val ssmClient = SsmClient.builder().region(Region.of(awsRegion)).build()
    try ssmClient.getParameter(GetParameterRequest.builder().name(name).withDecryption(true).build()).parameter().value()
    finally ssmClient.close()
  }

  private def printSummary(title: String, events: List[Event]): Unit = {
    println(s"\n=== $title ===")
    if (events.isEmpty) {
      println("  no samples")
    } else {
      events
        .groupBy(_.label.takeWhile(_ != '('))
        .toList
        .sortBy(_._1)
        .foreach { case (label, labelEvents) =>
          val durations = labelEvents.map(_.durationMs).sorted
          val failures = labelEvents.filterNot(_.outcome == okOutcome)
          val percentile95 = durations(Math.min(durations.size - 1, (durations.size * 95) / 100))
          println(
            f"  $label%-24s requests: ${labelEvents.size}%5d  min: ${durations.head}%7d ms  avg: ${durations.sum / durations.size}%7d ms  p95: $percentile95%7d ms  max: ${durations.last}%7d ms  non-200: ${failures.size}%4d"
          )
          failures.groupBy(_.outcome).toList.sortBy(_._1).foreach { case (outcome, outcomeEvents) => println(f"      ${outcomeEvents.size}%4d x $outcome") }
        }
    }
  }

  private def env(name: String, default: String): String = sys.env.get(name).map(_.trim).filter(_.nonEmpty).getOrElse(default)

  private case class Event(startedAtMs: Long, label: String, outcome: String, detail: String, durationMs: Long) {
    def describe: String = f"[${startedAtMs / 1000.0}%8.2fs] $label%-24s ${durationMs}%7d ms  $outcome" + (if (detail.isEmpty) "" else s"  $detail")
  }

  private class Timeline {
    private val startNanos = System.nanoTime()

    def elapsedMs: Long = (System.nanoTime() - startNanos) / 1000000

    def record(label: String, outcome: String, detail: String, durationMs: Long, startedAtMs: Long, print: Boolean): Event = {
      val event = Event(startedAtMs, label, outcome, detail, durationMs)
      if (print) synchronized(println(event.describe))
      event
    }

    def printHeader(consignmentId: UUID): Unit = println(
      s"""
         |=== File checks progress / health check investigation ===
         |API                  $apiBaseUrl
         |Auth                 $authUrl
         |Consignment          $consignmentId
         |Query                $queryType, $concurrentQueries concurrent for ${repeatFor.toSeconds}s
         |Health check poll    every ${pollInterval.toSeconds}s, timing out after ${healthCheckTimeout.toSeconds}s
         |Query timeout        ${queryTimeout.toSeconds}s
         |""".stripMargin
    )
  }
}
