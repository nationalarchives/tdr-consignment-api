import rocks.muki.graphql.quietError
import sbt.Keys.libraryDependencies

name := "tdr-consignment-api"
version := "0.1.0-SNAPSHOT"

description := "The consignment API for TDR"

scalaVersion := "2.13.12"
scalacOptions ++= Seq("-deprecation", "-feature")

(Compile / run / mainClass) := Some("uk.gov.nationalarchives.tdr.api.http.ApiServer")

graphqlSchemas += GraphQLSchema(
  "consignmentApi",
  "API schema from the schema.graphql file in the repository root",
  Def
    .task(
      GraphQLSchemaLoader
        .fromFile(baseDirectory.value.toPath.resolve("schema.graphql").toFile)
        .loadSchema()
    )
    .taskValue
)

val graphqlValidateSchemaTask = Def.inputTask[Unit] {
  val log = streams.value.log
  val changes = graphqlSchemaChanges.evaluated
  if (changes.nonEmpty) {
    changes.foreach(change => log.error(s" * ${change.description}"))
    quietError("Validation failed: Changes found")
  }
}

graphqlValidateSchema := graphqlValidateSchemaTask.evaluated

enablePlugins(GraphQLSchemaPlugin)

graphqlSchemaSnippet := "uk.gov.nationalarchives.tdr.api.graphql.GraphQlTypes.schema"

lazy val akkaHttpVersion = "10.5.0"
lazy val circeVersion = "0.14.6"
lazy val testContainersVersion = "0.41.0"
val http4sVersion = "0.23.23"

libraryDependencies ++= Seq(
  "org.sangria-graphql" %% "sangria" % "4.0.2",
  "org.sangria-graphql" %% "sangria-slowlog" % "3.0.0",
  "org.sangria-graphql" %% "sangria-circe" % "1.3.2",
  "org.sangria-graphql" %% "sangria-spray-json" % "1.0.3",
  "org.sangria-graphql" %% "sangria-relay" % "4.0.0",
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "de.heikoseeberger" %% "akka-http-circe" % "1.39.2",
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-xml" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream" % "2.8.0",
  "org.http4s" %% "http4s-core" % http4sVersion,
  "org.http4s" %% "http4s-circe" % http4sVersion,
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-ember-server" % http4sVersion,
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-optics" % "0.15.0",
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-generic-extras" % "0.14.3",
  "com.softwaremill.sttp.client3" %% "core" % "3.9.0",
  "uk.gov.nationalarchives" %% "consignment-api-db" % "0.1.38",
  "org.postgresql" % "postgresql" % "42.6.0",
  "com.typesafe.slick" %% "slick" % "3.4.1",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.4.1",
  "ch.megard" %% "akka-http-cors" % "1.2.0",
  "ch.qos.logback" % "logback-classic" % "1.4.11",
  "net.logstash.logback" % "logstash-logback-encoder" % "7.4",
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "5.0.0",
  "software.amazon.awssdk" % "rds" % "2.21.0",
  "software.amazon.awssdk" % "sts" % "2.21.0",
  "com.github.cb372" %% "scalacache-caffeine" % "0.28.0",
  "uk.gov.nationalarchives.oci" % "oci-tools-scala_2.13" % "0.3.0",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.typelevel" %% "cats-effect" % "3.5.2",
  "org.mockito" %% "mockito-scala" % "1.17.27" % Test,
  "org.mockito" %% "mockito-scala-scalatest" % "1.17.27" % Test,
  "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % Test,
  "com.typesafe.akka" %% "akka-testkit" % "2.8.0" % Test,
  "com.tngtech.keycloakmock" % "mock" % "0.16.0" % Test,
  "uk.gov.nationalarchives" %% "tdr-auth-utils" % "0.0.170",
  "io.github.hakky54" % "logcaptor" % "2.9.0" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % testContainersVersion % Test,
  "com.dimafeng" %% "testcontainers-scala-postgresql" % testContainersVersion % Test
)

dependencyOverrides ++= Seq(
  "com.typesafe.slick" %% "slick" % "3.4.1",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.4.1",
  "org.sangria-graphql" %% "sangria" % "3.5.3"
)

(Test / javaOptions) += s"-Dconfig.file=${sourceDirectory.value}/test/resources/application.conf"
(Test / fork) := true
(assembly / assemblyJarName) := "consignmentapi.jar"

(assembly / assemblyMergeStrategy) := {
  case PathList("META-INF", x, xs @ _*) if x.toLowerCase == "services" => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*)                                   => MergeStrategy.discard
  case PathList("reference.conf")                                      => MergeStrategy.concat
  case _                                                               => MergeStrategy.first
}

(assembly / mainClass) := Some("uk.gov.nationalarchives.tdr.api.http.ApiServer")
