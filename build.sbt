name := "strider"
organization := "com.outr"
version := "0.1.0-SNAPSHOT"

scalaVersion := "3.8.3"

versionScheme := Some("early-semver")

scalacOptions ++= Seq("-deprecation", "-feature", "-Wconf:any:silent")

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

fork := true

resolvers += "jitpack" at "https://jitpack.io"

evictionErrorLevel := Level.Warn

// Dependency versions
val spiceVersion = "1.6.0"
val lightdbVersion = "4.31.0-SNAPSHOT"
val rapidVersion = "2.9.2"
val scalatestVersion = "3.2.20"

libraryDependencies ++= Seq(
  "com.outr" %% "lightdb-all" % lightdbVersion,
  "com.outr" %% "rapid-core" % rapidVersion,
  "com.outr" %% "spice-server-undertow" % spiceVersion,
  "com.outr" %% "spice-openapi" % spiceVersion,

  "org.scalatest" %% "scalatest" % scalatestVersion % Test,
  "com.outr" %% "rapid-test" % rapidVersion % Test
)

licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
homepage := Some(url("https://github.com/outr/strider"))

developers := List(
  Developer(
    id = "darkfrog26",
    name = "Matt Hicks",
    email = "matt@matthicks.com",
    url = url("https://matthicks.com")
  )
)

scmInfo := Some(
  ScmInfo(
    url("https://github.com/outr/strider"),
    "scm:git@github.com:outr/strider.git"
  )
)
