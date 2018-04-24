name := "spark-csv"
version := "1.0-SPARK-2.3.0-SNAPSHOT"
organization := "org.apache.spark"

crossScalaVersions := Seq("2.11.11", "2.10.6")

scalaVersion := crossScalaVersions.value.head

spName := "apache/spark-csv"

sparkVersion := "2.3.0"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

sparkComponents := Seq("core", "sql", "hive")

resolvers ++= Seq("jitpack" at "https://jitpack.io")

libraryDependencies ++= Seq(
  // dependencies for unit tests
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",  // was 2.2.4
  "org.apache.commons" % "commons-lang3" % "3.4" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % "test"
)

fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

publishTo := Some("Artifactory Realm" at "http://esi-components.esi-group.com/artifactory/snapshot")
credentials += Credentials(Path.userHome / ".m2" / ".credentials")
//credentials += Credentials("Artifactory Realm", "esi-components.esi-group.com", "", "")

pomExtra :=
  <url>https://github.com/esi-mineset/spark-csv</url>
    <licenses>
      <license>
        <name>Apache License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:esi-mineset/spark-csv.git</url>
      <connection>scm:git:git@github.com:esi-mineset/spark-csv.git</connection>
    </scm>
    <developers>
      <developer>
        <id>davidw76</id>
        <name>David Webb</name>
        <url>https://cloud.esi-group.com/analytics</url>
      </developer>
    </developers>

// Skip tests during assembly
test in assembly := {}

addArtifact(artifact in (Compile, assembly), assembly)

initialCommands += """
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.SparkContext._
  import org.apache.spark.sql.SQLContext
  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("Console").
    set("spark.app.id", "Console")   // To silence Metrics warning.
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  import org.apache.spark.sql.functions._    // for min, max, etc.
  """

fork := true

// -- MiMa binary compatibility checks ------------------------------------------------------------

mimaPreviousArtifacts := Set("com.crealytics" %% "spark-excel" % "0.0.1")
// ------------------------------------------------------------------------------------------------
