import rocks.muki.graphql.quietError
import sbt.Keys.libraryDependencies

name := "tdr-consignment-api"
version := "0.1.0-SNAPSHOT"

description := "The consignment API for TDR"

scalaVersion := "2.13.17"
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

lazy val pekkoVersion = "1.2.1"
lazy val pekkoHttpVersion = "1.3.0"
lazy val circeVersion = "0.14.15"
lazy val testContainersVersion = "0.43.0"

libraryDependencies ++= Seq(
  "org.sangria-graphql" %% "sangria" % "4.2.15",
  "org.sangria-graphql" %% "sangria-slowlog" % "3.0.0",
  "org.sangria-graphql" %% "sangria-circe" % "1.3.2",
  "org.sangria-graphql" %% "sangria-spray-json" % "1.0.3",
  "org.sangria-graphql" %% "sangria-relay" % "4.0.1",
  "org.apache.pekko" %% "pekko-stream" % pekkoVersion,
  "org.apache.pekko" %% "pekko-connectors-slick" % "1.2.0",
  "org.apache.pekko" %% "pekko-http" % pekkoHttpVersion,
  "org.apache.pekko" %% "pekko-http-spray-json" % pekkoHttpVersion,
  "org.apache.pekko" %% "pekko-http-xml" % pekkoHttpVersion,
  "org.apache.pekko" %% "pekko-http-testkit"% pekkoHttpVersion % Test,
  "org.apache.pekko" %% "pekko-stream-testkit" % pekkoVersion % Test,
  "com.softwaremill.sttp.shared" %% "pekko" % "1.5.0",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-optics" % "0.15.1",
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-generic-extras" % "0.14.4",
  "com.softwaremill.sttp.client3" %% "core" % "3.11.0",
  "uk.gov.nationalarchives" %% "consignment-api-db" % "0.1.56",
  "uk.gov.nationalarchives" %% "tdr-metadata-validation" % "0.0.186",
  "org.postgresql" % "postgresql" % "42.7.8",
  "com.typesafe.slick" %% "slick" % "3.6.1",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.6.1",
  "ch.qos.logback" % "logback-classic" % "1.5.21",
  "net.logstash.logback" % "logstash-logback-encoder" % "9.0",
  "software.amazon.awssdk" % "rds" % "2.34.5",
  "software.amazon.awssdk" % "sts" % "2.34.5",
  "com.github.cb372" %% "scalacache-caffeine" % "0.28.0",
  "uk.gov.nationalarchives.oci" % "oci-tools-scala_2.13" % "0.4.0",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.mockito" %% "mockito-scala" % "2.0.0" % Test,
  "org.mockito" %% "mockito-scala-scalatest" % "2.0.0" % Test,
  "com.tngtech.keycloakmock" % "mock" % "0.16.0" % Test,
  "uk.gov.nationalarchives" %% "tdr-auth-utils" % "0.0.259",
  "io.github.hakky54" % "logcaptor" % "2.9.0" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % testContainersVersion % Test,
  "com.dimafeng" %% "testcontainers-scala-postgresql" % testContainersVersion % Test,
  "com.github.tomakehurst" % "wiremock-standalone" % "3.0.1" % Test,
  "uk.gov.nationalarchives" % "da-metadata-schema_2.13" % "0.0.95"
)

dependencyOverrides ++= Seq(
  "com.typesafe.slick" %% "slick" % "3.5.2",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.5.2",
  "org.sangria-graphql" %% "sangria" % "4.0.2"
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
