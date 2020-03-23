resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")
addSbtPlugin("com.frugalmechanic" % "fm-sbt-s3-resolver" % "0.19.0")
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.2")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.15")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-license-report" % "1.2.0")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.8")
