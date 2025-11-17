name := "htx-topitems"
version := "0.1"
scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2" % Provided,
  "org.apache.spark" %% "spark-sql" % "3.3.2" % Provided,
  "org.scalatest" %% "scalatest" % "3.2.16" % Test
)

Test / fork := true

Test / javaOptions ++= Seq(
  "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"
)
