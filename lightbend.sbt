lazy val akkaToken = sys.env.getOrElse("AKKA_TOKEN", "XXXXX")

ThisBuild / resolvers += "lightbend-commercial-mvn" at s"https://repo.lightbend.com/pass/$akkaToken/commercial-releases"
ThisBuild / resolvers += Resolver.url("lightbend-commercial-ivy", url(s"https://repo.lightbend.com/pass/$akkaToken/commercial-releases"))(Resolver.ivyStylePatterns)
