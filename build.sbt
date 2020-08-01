import Dependencies._

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "credit_loss",
    libraryDependencies += scalaTest % Test
  )

//Uncomment the following for publishing to Sonatype.
//See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for more detail.

ThisBuild / description := "Credit loss distribution for a loan portfolio."
ThisBuild / licenses    := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage    := Some(url("https://github.com/phillyfan1138/CreditLoss"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/phillyfan1138/CreditLoss"),
    "scm:git@github.com:phillyfan1138/CreditLoss.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "phillyfan1138",
    name  = "Daniel Stahl",
    email = "danstahl1138@gmail.com",
    url   = url("https://danielhstahl.com")
  )
)
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true
