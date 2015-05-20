name := "SparkDemos"

version := "1.0"

scalaVersion := "2.10.4"


libraryDependencies ++=  Seq("org.apache.spark" %% "spark-core" % "1.3.0",
                              "org.apache.spark" %% "spark-sql" % "1.3.0",
                              "org.apache.spark" %% "spark-streaming" % "1.3.0",
                              "org.apache.spark" %% "spark-mllib" % "1.3.0",
                              "org.apache.spark" %% "spark-graphx" % "1.3.0",
                              "org.apache.spark" %% "spark-streaming-kafka" % "1.3.0" )

libraryDependencies ++= Seq(
  "com.couchbase.client" %% "spark-connector" % "1.0.0-dp",
  "org.apache.spark" %% "spark-streaming" % "1.2.1"
)

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-rc2"

resolvers += "Couchbase Repository" at "http://files.couchbase.com/maven2/"