name := "scala-utils"

organization := "com.kosmyna"

version := "1.0.0"

scalaVersion := "2.12.10"

//addCompilerPlugin(scalafixSemanticdb) // enable SemanticDB
// Coursier is disabled due to log4j resolving -- duplicate exists and it fails.
useCoursier := false

scalacOptions ++= Seq(
	"-deprecation",
	"-unchecked",
	"-release", "9",
	"-Ywarn-unused:imports",
	"-explaintypes",
	"-Ypartial-unification",
//	"-Yrangepos",  // enable SemanticDB
	"-Ywarn-unused-import",
//	"-Xlog-implicits"
)

javacOptions ++= Seq("-source", "1.9", "-target", "1.9")

// disable scaladoc
publishArtifact in (Compile, packageDoc) := false
//don't cross-compile for scala versions
crossPaths := false
parallelExecution in ThisBuild := false
Revolver.enableDebugging(port = 5005)


publishMavenStyle := true

val akkaVersion = "2.5.8"
val log4jVersion = "2.10.0"
val elastic4sVersion= "7.3.1"

libraryDependencies ++= Seq(
	"com.typesafe.akka" %% "akka-stream" % akkaVersion,

	// es for permission service
	//Elastic search client
	"com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
	"com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % elastic4sVersion,

	// Logging
	// the lazyLogging trait
	"com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",

	// Test
	"org.scalatest" %% "scalatest" % "3.0.1",
	"org.scalatest" %% "scalatest" % "3.0.1" % Test,
)

// Injected build info and git details into compile
val gitCommitString = SettingKey[String]("gitCommit")
gitCommitString := git.gitHeadCommit.value.getOrElse("Not Set")

// @see: https://brainchunk.blogspot.com/2016/11/embed-git-commit-hash-scala-project.html
lazy val root = (project in file(".")).
	enablePlugins(BuildInfoPlugin).
	settings(
		// Build info settings
		buildInfoPackage := "com.kosmyna.util",
		buildInfoKeys := Seq[BuildInfoKey](name, version, gitCommitString),
		buildInfoOptions += BuildInfoOption.ToMap,
		buildInfoOptions += BuildInfoOption.ToJson,
		buildInfoOptions += BuildInfoOption.BuildTime,
	)
