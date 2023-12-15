scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-mllib" % "2.4.8",
  "org.apache.spark" %% "spark-catalyst" % "2.4.8",
  "org.vegas-viz" %% "vegas-spark" % "0.3.11",
  "org.vegas-viz" %% "vegas" % "0.3.11"
)
