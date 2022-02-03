addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("rocks.muki" % "sbt-graphql" % "0.14.0")
addSbtPlugin("com.frugalmechanic" % "fm-sbt-s3-resolver" % "0.19.0")
resolvers += "Sonatype OSS" at "https://s01.oss.sonatype.org/content/groups/public/"

addSbtPlugin("io.github.sfali23" % "sbt-semver-release"  % "0.2.0")