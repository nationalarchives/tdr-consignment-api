import rocks.muki.graphql.schema.SchemaLoader
import sbt.File

name := "tdr-consignment-api"
version := "0.1.0-SNAPSHOT"

description := "The consignment API for TDR"

scalaVersion := "2.13.0"
scalacOptions ++= Seq("-deprecation", "-feature")

resolvers ++= Seq(
  "Sonatype Releases" at "https://dl.bintray.com/mockito/maven/"
)

mainClass in (Compile, run) := Some("uk.gov.nationalarchives.tdr.api.http.ApiServer")

enablePlugins(GraphQLSchemaPlugin)

graphqlSchemaSnippet := "uk.gov.nationalarchives.tdr.api.graphql.GraphQlTypes.schema"

lazy val akkaHttpVersion = "10.1.11"

libraryDependencies ++= Seq(
  "org.sangria-graphql" %% "sangria" % "2.0.0-M3",
  "org.sangria-graphql" %% "sangria-slowlog" % "2.0.0-M1",
  "org.sangria-graphql" %% "sangria-circe" % "1.3.0",
  "org.sangria-graphql" %% "sangria-spray-json" % "1.0.2",

  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "de.heikoseeberger" %% "akka-http-circe" % "1.30.0",
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-xml"        % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream"          % "2.6.3",

  "io.circe" %% "circe-core" % "0.13.0-M2",
  "io.circe" %% "circe-parser" % "0.13.0-M2",
  "io.circe" %% "circe-optics" % "0.12.0",
  "io.circe" %% "circe-generic" % "0.13.0-M2",
  "uk.gov.nationalarchives" %% "consignment-api-db" % "0.0.3",
  "mysql" % "mysql-connector-java" % "6.0.6",
  "com.typesafe.slick" %% "slick" % "3.3.2",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.3.2",
  "ch.megard" %% "akka-http-cors" % "0.4.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.jboss.logging" % "jboss-logging" % "3.4.1.Final",
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "1.1.2",
  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  "org.mockito" %% "mockito-scala" % "1.6.3" % Test,
  "org.mockito" %% "mockito-scala-scalatest" % "1.6.3" % Test,
  "com.typesafe.akka" %% "akka-http-testkit" % "10.1.11" % Test,
  "com.typesafe.akka" %% "akka-testkit" % "2.6.3" % Test,
  "com.tngtech.keycloakmock" % "mock" % "0.2.0" % Test,
  "com.h2database" % "h2" % "1.4.200" % Test,
  "uk.gov.nationalarchives.tdr" %% "tdr-auth-utils" % "0.1.0-SNAPSHOT"
)

javaOptions in Test += s"-Dconfig.file=${sourceDirectory.value}/test/resources/application.conf"
fork in Test := true

assemblyJarName in assembly := "consignmentapi.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
