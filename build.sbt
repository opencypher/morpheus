resolvers in Global ++= Seq(
  "neo4j-snapshots" at "https://m2.neo4j.org/content/repositories/snapshots",
  "opencypher-public" at "https://neo.jfrog.io/neo/opencypher-public")

scalaVersion in Global := "2.11.12"

scalacOptions in ThisBuild ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature",
  "-Xfatal-warnings",
  "-Xfuture",
  "-Ywarn-adapted-args",
  "-Yopt-warnings:at-inline-failed",
  "-Yopt:l:project",
  "-Ypartial-unification"
)

name := "okapi"

version in ThisBuild := "1.0-SNAPSHOT"

organization in ThisBuild := "org.opencypher"

libraryDependencies in Global ++= Seq(
  `org.scala-lang_scala-compiler`,
  `org.scala-lang_scala-reflect`,
  `org.scalatest_scalatest` % "test",
  `org.scalacheck_scalacheck` % "test",
  `org.neo4j.test_neo4j-harness` % "test",
  `org.neo4j_openCypher-frontend-1` % "test" classifier "tests",
  `org.neo4j_neo4j-cypher-util-3.4` % "test" classifier "tests",
  `org.mockito_mockito-all` % "test",
  `org.junit.platform_junit-platform-runner` % "test")

// Main project
lazy val sparkCypher = project.in(file("spark-cypher")).settings(
  libraryDependencies ++= Seq(
    `org.apache.spark_spark-sql`,
    `org.apache.spark_spark-core`,
    `org.apache.spark_spark-catalyst`)
    ++ miniclusterTestDependencies
  ).dependsOn(okapiApi, okapiIr, okapiLogical, okapiRelational, okapiTrees)

// Pipeline
lazy val okapiRelational = project.in(file("okapi-relational")).settings(
  libraryDependencies ++= Seq(
    `org.neo4j.driver_neo4j-java-driver`,
    `io.circe_circe-parser`,
    `io.circe_circe-generic`,
    `io.circe_circe-core`
  )
).dependsOn(okapiApi, okapiIr, okapiLogical, okapiLogical, okapiTrees)

lazy val okapiLogical = project.in(file("okapi-logical")).settings(
).dependsOn(okapiApi, okapiIr, okapiTrees)

lazy val okapiIr = project.in(file("okapi-ir")).settings(
  libraryDependencies ++= Seq(
    `org.neo4j_openCypher-frontend-1`,
    `org.atnos_eff`)
).dependsOn(okapiApi, okapiTrees)

lazy val okapiApi = project.in(file("okapi-api")).settings(
  libraryDependencies ++= Seq(`org.typelevel_cats-core`)
).dependsOn(okapiTrees, okapiTrees)

// TCK
lazy val sparkCypherTck = project.in(file("spark-cypher-tck"))
  .settings(
    libraryDependencies ++= Seq(
      `org.opencypher_tck` % "test"
    ),
    name := "spark-cypher-tck"
  ).dependsOn(okapiApi, okapiIr, okapiTck, sparkCypher)

lazy val okapiTck = project.in(file("okapi-tck")).settings(
  libraryDependencies ++= Seq(
    `org.opencypher_tck` % "test")
).dependsOn(okapiApi, okapiIr, okapiIr)

// Examples
lazy val sparkCypherExamples = project.in(file("spark-cypher-examples")).settings(
  libraryDependencies ++= Seq(
    `org.apache.spark_spark-graphx`)
).dependsOn(sparkCypher)

// Trees
lazy val okapiTrees = project.in(file("okapi-trees"))

// Dependencies
lazy val `io.circe_circe-core` = "io.circe" %% "circe-core" % "0.9.1"

lazy val `io.circe_circe-generic` = "io.circe" %% "circe-generic" % "0.9.1"

lazy val `io.circe_circe-parser` = "io.circe" %% "circe-parser" % "0.9.1"

lazy val `org.apache.spark_spark-core` = "org.apache.spark" %% "spark-core" % "2.2.0"

lazy val `org.apache.spark_spark-sql` = "org.apache.spark" %% "spark-sql" % "2.2.0"

lazy val `org.apache.spark_spark-graphx` = "org.apache.spark" %% "spark-graphx" % "2.2.0"

lazy val `org.apache.spark_spark-catalyst` = "org.apache.spark" %% "spark-catalyst" % "2.2.0"

lazy val `org.atnos_eff` = "org.atnos" %% "eff" % "5.0.0"

lazy val `org.junit.platform_junit-platform-runner` = "org.junit.platform" % "junit-platform-runner" % "1.0.2"

lazy val `org.mockito_mockito-all` = "org.mockito" % "mockito-all" % "1.10.19"

lazy val `org.neo4j_neo4j-cypher-util-3.4` = "org.neo4j" % "neo4j-cypher-util-3.4" % "3.4.0-SNAPSHOT"

lazy val `org.neo4j_openCypher-frontend-1` = "org.neo4j" % "openCypher-frontend-1" % "3.4.0-SNAPSHOT"

lazy val `org.neo4j.driver_neo4j-java-driver` = "org.neo4j.driver" % "neo4j-java-driver" % "1.4.2"

lazy val `org.neo4j.test_neo4j-harness` = "org.neo4j.test" % "neo4j-harness" % "3.3.3"

lazy val `org.opencypher_tck` = "org.opencypher" % "tck" % "1.0.0-M08"

lazy val `org.scala-lang_scala-compiler` = "org.scala-lang" % "scala-compiler" % "2.11.12"

lazy val `org.scala-lang_scala-reflect` = "org.scala-lang" % "scala-reflect" % "2.11.12"

lazy val `org.scalacheck_scalacheck` = "org.scalacheck" %% "scalacheck" % "1.13.5"

lazy val `org.scalatest_scalatest` = "org.scalatest" %% "scalatest" % "3.0.5"

lazy val `org.typelevel_cats-core` = "org.typelevel" %% "cats-core" % "1.0.1"

// Workaround for Minicluster test dependency problem: https://github.com/sbt/sbt/issues/2964
lazy val miniclusterTestDependencies = Seq(
  "org.apache.hadoop" % "hadoop-minicluster" % "2.7.3" % "test",
//  "org.apache.hadoop" % "hadoop-minicluster" % "2.7.3" % Test classifier "tests",
  "org.apache.hbase" % "hbase-common" % "1.2.2" % Test classifier "tests",
  "org.apache.hbase" % "hbase-common" % "1.2.2" % Test,
  "org.apache.hbase" % "hbase-server" % "1.2.2" % Test classifier "tests",
  "org.apache.hbase" % "hbase-server" % "1.2.2" % Test,
  "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.2" % Test classifier "tests",
  "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.2" % Test,
  "org.apache.hbase" % "hbase-hadoop2-compat" % "1.2.2" % Test classifier "tests",
  "org.apache.hbase" % "hbase-hadoop2-compat" % "1.2.2" % Test,
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3" % Test classifier "tests",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3" % Test,
  "org.apache.hadoop" % "hadoop-common" % "2.7.3" % Test,
  "org.apache.hadoop" % "hadoop-common" % "2.7.3" % Test classifier "tests",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.7.3" % Test classifier "tests")
