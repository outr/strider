name := "strider"

ThisBuild / organization := "com.outr"
ThisBuild / version := "1.0.0"

ThisBuild / scalaVersion := "3.8.3"

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-Wconf:any:silent")

ThisBuild / Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

ThisBuild / fork := true

ThisBuild / resolvers += "jitpack" at "https://jitpack.io"

ThisBuild / evictionErrorLevel := Level.Warn

ThisBuild / licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT"))
ThisBuild / homepage := Some(url("https://github.com/outr/strider"))

ThisBuild / developers := List(
  Developer(
    id = "darkfrog26",
    name = "Matt Hicks",
    email = "matt@matthicks.com",
    url = url("https://matthicks.com")
  )
)

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/outr/strider"),
    "scm:git@github.com:outr/strider.git"
  )
)

// Dependency versions
val spiceVersion = "1.7.0"
val lightdbVersion = "4.31.0"
val rapidVersion = "2.9.2"
val scalatestVersion = "3.2.20"

lazy val strider = project.in(file("."))
  .settings(
    name := "strider",
    libraryDependencies ++= Seq(
      "com.outr" %% "lightdb-all" % lightdbVersion,
      "com.outr" %% "rapid-core" % rapidVersion,
      "com.outr" %% "spice-server-undertow" % spiceVersion,
      "com.outr" %% "spice-openapi" % spiceVersion,

      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "com.outr" %% "rapid-test" % rapidVersion % Test
    )
  )

lazy val docs = project
  .in(file("documentation"))
  .dependsOn(strider)
  .enablePlugins(MdocPlugin)
  .settings(
    publish / skip := true,
    mdocVariables := Map(
      "VERSION" -> version.value
    ),
    mdocIn := (ThisBuild / baseDirectory).value / "docs",
    mdocOut := (ThisBuild / baseDirectory).value
  )
