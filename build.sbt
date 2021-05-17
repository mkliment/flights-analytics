name := "flights-analytics"
version := "0.0.1-SNAPSHOT"
organization := "kliment.markovski"

scalaVersion := "2.12.12"
val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
    "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
    "com.github.scopt" %% "scopt"      % "3.7.1"      % Compile,
    "org.scala-lang"   %  "scala-reflect" % "2.12.12",
    "org.scalatest"    %% "scalatest"  % "3.2.2"      % "test, it",
    "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % Test
)

assemblyMergeStrategy in assembly := {
    case PathList("reference.conf")          => MergeStrategy.concat
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case PathList("plugin.properties")       => MergeStrategy.last
    case PathList("log4j.properties")        => MergeStrategy.last
    case _                                   => MergeStrategy.first
}

resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.jcenterRepo

// test run settings
//parallelExecution in Test := false
assembly / test := {}

// Enable integration tests
Defaults.itSettings
lazy val root = project.in(file(".")).configs(IntegrationTest)

// Measure time for each test
Test / testOptions += Tests.Argument("-oD")
IntegrationTest / testOptions += Tests.Argument("-oD")

// Scoverage settings
coverageExcludedPackages := "<empty>;.*storage.*"
coverageMinimum := 70
coverageFailOnMinimum := true

// Scalastyle settings
scalastyleFailOnWarning := false
scalastyleFailOnError := true
