// Variables
val org: String = "com.outr"
val projectName: String = "strider"
val githubOrg: String = "outr"
val email: String = "matt@matthicks.com"
val developerId: String = "darkfrog"
val developerName: String = "Matt Hicks"
val developerURL: String = "https://matthicks.com"

name := projectName

ThisBuild / organization := org
ThisBuild / version := "1.1.1"

ThisBuild / scalaVersion := "3.8.3"

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-Wconf:any:silent")

ThisBuild / Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

ThisBuild / fork := true

ThisBuild / resolvers += "jitpack" at "https://jitpack.io"

ThisBuild / evictionErrorLevel := Level.Warn

// Publishing
publishMavenStyle := true

ThisBuild / sonatypeCredentialHost := xerial.sbt.Sonatype.sonatypeCentralHost
ThisBuild / publishTo := sonatypePublishToBundle.value
ThisBuild / sonatypeProfileName := org
ThisBuild / licenses := Seq("MIT" -> url(s"https://github.com/$githubOrg/$projectName/blob/main/LICENSE"))
ThisBuild / sonatypeProjectHosting := Some(xerial.sbt.Sonatype.GitHubHosting(githubOrg, projectName, email))
ThisBuild / homepage := Some(url(s"https://github.com/$githubOrg/$projectName"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url(s"https://github.com/$githubOrg/$projectName"),
    s"scm:git@github.com:$githubOrg/$projectName.git"
  )
)
ThisBuild / developers := List(
  Developer(id = developerId, name = developerName, email = email, url = url(developerURL))
)

// Dependency versions
val spiceVersion = "1.9.0"

val lightdbVersion = "4.40.0"

val rapidVersion = "2.9.6"

val scalatestVersion = "3.2.20"

lazy val strider = project.in(file("."))
  .settings(
    name := projectName,
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
