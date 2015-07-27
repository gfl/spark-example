name := "spark-example"

version := "1.0"

scalaVersion := "2.11.6"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.4.1",
  "org.apache.spark" %% "spark-hive" % "1.4.1",
  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
)

resourceDirectory in Compile := baseDirectory.value / "resources"


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"