name := "spark-example"

version := "1.0"

scalaVersion := "2.11.6"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
)

resourceDirectory in Compile := baseDirectory.value / "resources"


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"