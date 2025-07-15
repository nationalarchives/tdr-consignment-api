lazy val akkaToken = sys.env.getOrElse("AKKA_TOKEN", "dummyTokenValue")

ThisBuild / resolvers += "akka-secure-mvn" at s"https://repo.akka.io/$akkaToken/secure"
ThisBuild / resolvers += Resolver.url("akka-secure-ivy", url(s"https://repo.akka.io/$akkaToken/secure"))(Resolver.ivyStylePatterns)
