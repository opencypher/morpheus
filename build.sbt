scalaVersion in Global := "2.11.12" 

def ProjectName(name: String,path:String): Project =  Project(name, file(path))

resolvers in Global ++= Seq(Resolver.mavenLocal, "https://m2.neo4j.org/content/repositories/snapshots" at "https://m2.neo4j.org/content/repositories/snapshots" ,
           "https://neo.jfrog.io/neo/opencypher-public" at "https://neo.jfrog.io/neo/opencypher-public" ,
           "https://repo.maven.apache.org/maven2" at "https://repo.maven.apache.org/maven2" )

val `com.lihaoyi_ammonite_2.11.12` = "com.lihaoyi" % "ammonite_2.11.12" % "1.0.3-33-2d70b25"

val `com.lihaoyi_ammonite-ops_2.11` = "com.lihaoyi" % "ammonite-ops_2.11" % "1.0.3-33-2d70b25"

val `io.circe_circe-core_2.11` = "io.circe" % "circe-core_2.11" % "0.9.1"

val `io.circe_circe-generic_2.11` = "io.circe" % "circe-generic_2.11" % "0.9.1"

val `io.circe_circe-parser_2.11` = "io.circe" % "circe-parser_2.11" % "0.9.1"

val `io.github.nicolasstucki_multisets_2.11` = "io.github.nicolasstucki" % "multisets_2.11" % "0.4"

val `org.apache.hadoop_hadoop-minicluster` = "org.apache.hadoop" % "hadoop-minicluster" % "2.7.0"

val `org.apache.spark_spark-core_2.11` = "org.apache.spark" % "spark-core_2.11" % "2.2.0"

val `org.apache.spark_spark-sql_2.11` = "org.apache.spark" % "spark-sql_2.11" % "2.2.0"

val `org.apache.spark_spark-graphx_2.11` = "org.apache.spark" % "spark-graphx_2.11" % "2.2.0"

val `org.apache.spark_spark-catalyst_2.11` = "org.apache.spark" % "spark-catalyst_2.11" % "2.2.0"

val `org.atnos_eff_2.11` = "org.atnos" % "eff_2.11" % "5.0.0"

val `org.bouncycastle_bctls-jdk15on` = "org.bouncycastle" % "bctls-jdk15on" % "1.57"

val `org.junit.platform_junit-platform-runner` = "org.junit.platform" % "junit-platform-runner" % "1.0.2"

val `org.mockito_mockito-all` = "org.mockito" % "mockito-all" % "1.10.19"

val `org.neo4j_neo4j-cypher-util-3.4` = "org.neo4j" % "neo4j-cypher-util-3.4" % "3.4.0-SNAPSHOT"

val `org.neo4j_openCypher-frontend-1` = "org.neo4j" % "openCypher-frontend-1" % "3.4.0-SNAPSHOT"

val `org.neo4j.driver_neo4j-java-driver` = "org.neo4j.driver" % "neo4j-java-driver" % "1.4.2"

val `org.neo4j.test_neo4j-harness` = "org.neo4j.test" % "neo4j-harness" % "3.3.3"

val `org.opencypher_tck` = "org.opencypher" % "tck" % "1.0.0-M08"

val `org.scala-lang_scala-compiler` = "org.scala-lang" % "scala-compiler" % "2.11.12"

val `org.scala-lang_scala-library` = "org.scala-lang" % "scala-library" % "2.11.12"

val `org.scala-lang_scala-reflect` = "org.scala-lang" % "scala-reflect" % "2.11.12"

val `org.scalacheck_scalacheck_2.11` = "org.scalacheck" % "scalacheck_2.11" % "1.13.5"

val `org.scalatest_scalatest_2.11` = "org.scalatest" % "scalatest_2.11" % "3.0.5"

val `org.typelevel_cats-core_2.11` = "org.typelevel" % "cats-core_2.11" % "1.0.1"

lazy val `spark-cypher-tck` = ProjectName("spark-cypher-tck","spark-cypher-tck").settings(
  libraryDependencies ++= Seq(`org.scalatest_scalatest_2.11` % "test" ,
   `org.scalacheck_scalacheck_2.11` % "test" ,
   `org.scala-lang_scala-reflect`,
   `org.scala-lang_scala-library`,
   `org.scala-lang_scala-compiler`,
   `org.opencypher_tck` % "test" ,
   `org.neo4j.test_neo4j-harness` % "test" ,
   `org.neo4j_openCypher-frontend-1` % "test" classifier "tests" ,
   `org.neo4j_neo4j-cypher-util-3.4` % "test" classifier "tests" ,
   `org.mockito_mockito-all` % "test" ,
   `org.junit.platform_junit-platform-runner` % "test" ,
   `org.bouncycastle_bctls-jdk15on` % "test" ,
   `org.apache.hadoop_hadoop-minicluster` % "test" ,
   `io.github.nicolasstucki_multisets_2.11` % "test" ),
    name := "spark-cypher-tck",
    version := "1.0-SNAPSHOT",
    organization := "org.opencypher"
).settings().dependsOn(`okapi-api`% "test -> test" ,`okapi-ir`% "test -> test" ,`okapi-tck`% "test -> test" ,`spark-cypher`% "test -> test" ,`spark-cypher`% "test -> test" )

lazy val `spark-cypher-examples` = ProjectName("spark-cypher-examples","spark-cypher-examples").settings(
  libraryDependencies ++= Seq(`org.scalatest_scalatest_2.11` % "test" ,
   `org.scalacheck_scalacheck_2.11` % "test" ,
   `org.neo4j.test_neo4j-harness` % "test" ,
   `org.neo4j_openCypher-frontend-1` % "test" classifier "tests" ,
   `org.neo4j_neo4j-cypher-util-3.4` % "test" classifier "tests" ,
   `org.mockito_mockito-all` % "test" ,
   `org.junit.platform_junit-platform-runner` % "test" ,
   `org.bouncycastle_bctls-jdk15on` % "test" ,
   `org.apache.spark_spark-graphx_2.11`,
   `org.apache.hadoop_hadoop-minicluster` % "test" ,
   `io.github.nicolasstucki_multisets_2.11` % "test" ),
    name := "spark-cypher-examples",
    version := "1.0-SNAPSHOT",
    organization := "org.opencypher"
).settings().dependsOn(`spark-cypher`)

lazy val `spark-cypher` = ProjectName("spark-cypher","spark-cypher").settings(
  libraryDependencies ++= Seq(`org.scalatest_scalatest_2.11` % "test" ,
   `org.scalacheck_scalacheck_2.11` % "test" ,
   `org.neo4j.test_neo4j-harness` % "test" ,
   `org.neo4j_openCypher-frontend-1` % "test" classifier "tests" ,
   `org.neo4j_neo4j-cypher-util-3.4` % "test" classifier "tests" ,
   `org.mockito_mockito-all` % "test" ,
   `org.junit.platform_junit-platform-runner` % "test" ,
   `org.bouncycastle_bctls-jdk15on` % "test" ,
   `org.apache.spark_spark-sql_2.11`,
   `org.apache.spark_spark-core_2.11`,
   `org.apache.spark_spark-catalyst_2.11`,
   `org.apache.hadoop_hadoop-minicluster` % "test" ,
   `io.github.nicolasstucki_multisets_2.11` % "test" ),
    name := "spark-cypher",
    version := "1.0-SNAPSHOT",
    organization := "org.opencypher"
).settings().dependsOn(`okapi-api`% "test -> test" ,`okapi-ir`% "test -> test" ,`okapi-logical`% "test -> test" ,`okapi-relational`,`okapi-relational`% "test -> test" ,`okapi-trees`% "test -> test" )

lazy val `okapi-trees` = ProjectName("okapi-trees","okapi-trees").settings(
  libraryDependencies ++= Seq(`org.scalatest_scalatest_2.11` % "test" ,
   `org.scalacheck_scalacheck_2.11` % "test" ,
   `org.scala-lang_scala-reflect`,
   `org.scala-lang_scala-library`,
   `org.scala-lang_scala-compiler`,
   `org.neo4j.test_neo4j-harness` % "test" ,
   `org.neo4j_openCypher-frontend-1` % "test" classifier "tests" ,
   `org.neo4j_neo4j-cypher-util-3.4` % "test" classifier "tests" ,
   `org.mockito_mockito-all` % "test" ,
   `org.junit.platform_junit-platform-runner` % "test" ,
   `org.bouncycastle_bctls-jdk15on` % "test" ,
   `org.apache.hadoop_hadoop-minicluster` % "test" ,
   `io.github.nicolasstucki_multisets_2.11` % "test" ),
    name := "okapi-trees",
    version := "1.0-SNAPSHOT",
    organization := "org.opencypher"
).settings().dependsOn()

lazy val `okapi-tck` = ProjectName("okapi-tck","okapi-tck").settings(
  libraryDependencies ++= Seq(`org.scalatest_scalatest_2.11` % "test" ,
   `org.scalacheck_scalacheck_2.11` % "test" ,
   `org.scala-lang_scala-reflect`,
   `org.scala-lang_scala-library`,
   `org.scala-lang_scala-compiler`,
   `org.opencypher_tck` % "test" ,
   `org.neo4j.test_neo4j-harness` % "test" ,
   `org.neo4j_openCypher-frontend-1` % "test" classifier "tests" ,
   `org.neo4j_neo4j-cypher-util-3.4` % "test" classifier "tests" ,
   `org.mockito_mockito-all` % "test" ,
   `org.junit.platform_junit-platform-runner` % "test" ,
   `org.bouncycastle_bctls-jdk15on` % "test" ,
   `org.apache.hadoop_hadoop-minicluster` % "test" ,
   `io.github.nicolasstucki_multisets_2.11` % "test" ),
    name := "okapi-tck",
    version := "1.0-SNAPSHOT",
    organization := "org.opencypher"
).settings().dependsOn(`okapi-api`% "test -> test" ,`okapi-ir`% "test -> test" ,`okapi-ir`% "test -> test" )

lazy val `okapi-relational` = ProjectName("okapi-relational","okapi-relational").settings(
  libraryDependencies ++= Seq(`org.scalatest_scalatest_2.11` % "test" ,
   `org.scalacheck_scalacheck_2.11` % "test" ,
   `org.neo4j.test_neo4j-harness` % "test" ,
   `org.neo4j.driver_neo4j-java-driver`,
   `org.neo4j_openCypher-frontend-1` % "test" classifier "tests" ,
   `org.neo4j_neo4j-cypher-util-3.4` % "test" classifier "tests" ,
   `org.mockito_mockito-all` % "test" ,
   `org.junit.platform_junit-platform-runner` % "test" ,
   `org.bouncycastle_bctls-jdk15on` % "test" ,
   `org.apache.hadoop_hadoop-minicluster` % "test" ,
   `io.github.nicolasstucki_multisets_2.11` % "test" ,
   `io.circe_circe-parser_2.11`,
   `io.circe_circe-generic_2.11`,
   `io.circe_circe-core_2.11`,
   `com.lihaoyi_ammonite_2.11.12`,
   `com.lihaoyi_ammonite-ops_2.11`),
    name := "okapi-relational",
    version := "1.0-SNAPSHOT",
    organization := "org.opencypher"
).settings().dependsOn(`okapi-api`% "test -> test" ,`okapi-ir`% "test -> test" ,`okapi-logical`% "test -> test" ,`okapi-logical`,`okapi-trees`% "test -> test" )

lazy val `okapi-logical` = ProjectName("okapi-logical","okapi-logical").settings(
  libraryDependencies ++= Seq(`org.scalatest_scalatest_2.11` % "test" ,
   `org.scalacheck_scalacheck_2.11` % "test" ,
   `org.neo4j.test_neo4j-harness` % "test" ,
   `org.neo4j_openCypher-frontend-1` % "test" classifier "tests" ,
   `org.neo4j_neo4j-cypher-util-3.4` % "test" classifier "tests" ,
   `org.mockito_mockito-all` % "test" ,
   `org.junit.platform_junit-platform-runner` % "test" ,
   `org.bouncycastle_bctls-jdk15on` % "test" ,
   `org.apache.hadoop_hadoop-minicluster` % "test" ,
   `io.github.nicolasstucki_multisets_2.11` % "test" ),
    name := "okapi-logical",
    version := "1.0-SNAPSHOT",
    organization := "org.opencypher"
).settings().dependsOn(`okapi-api`% "test -> test" ,`okapi-ir`% "test -> test" ,`okapi-ir`,`okapi-trees`% "test -> test" )

lazy val `okapi-ir` = ProjectName("okapi-ir","okapi-ir").settings(
  libraryDependencies ++= Seq(`org.scalatest_scalatest_2.11` % "test" ,
   `org.scalacheck_scalacheck_2.11` % "test" ,
   `org.neo4j.test_neo4j-harness` % "test" ,
   `org.neo4j_openCypher-frontend-1`,
   `org.neo4j_openCypher-frontend-1` % "test" classifier "tests" ,
   `org.neo4j_neo4j-cypher-util-3.4` % "test" classifier "tests" ,
   `org.mockito_mockito-all` % "test" ,
   `org.junit.platform_junit-platform-runner` % "test" ,
   `org.bouncycastle_bctls-jdk15on` % "test" ,
   `org.atnos_eff_2.11`,
   `org.apache.hadoop_hadoop-minicluster` % "test" ,
   `io.github.nicolasstucki_multisets_2.11` % "test" ),
    name := "okapi-ir",
    version := "1.0-SNAPSHOT",
    organization := "org.opencypher"
).settings().dependsOn(`okapi-api`% "test -> test" ,`okapi-api`,`okapi-trees`% "test -> test" )

lazy val `okapi-api` = ProjectName("okapi-api","okapi-api").settings(
  libraryDependencies ++= Seq(`org.typelevel_cats-core_2.11`,
   `org.scalatest_scalatest_2.11` % "test" ,
   `org.scalacheck_scalacheck_2.11` % "test" ,
   `org.neo4j.test_neo4j-harness` % "test" ,
   `org.neo4j_openCypher-frontend-1` % "test" classifier "tests" ,
   `org.neo4j_neo4j-cypher-util-3.4` % "test" classifier "tests" ,
   `org.mockito_mockito-all` % "test" ,
   `org.junit.platform_junit-platform-runner` % "test" ,
   `org.bouncycastle_bctls-jdk15on` % "test" ,
   `org.apache.hadoop_hadoop-minicluster` % "test" ,
   `io.github.nicolasstucki_multisets_2.11` % "test" ),
    name := "okapi-api",
    version := "1.0-SNAPSHOT",
    organization := "org.opencypher"
).settings().dependsOn(`okapi-trees`% "test -> test" ,`okapi-trees`)


version := "1.0-SNAPSHOT"

name := "okapi"

organization := "org.opencypher"

libraryDependencies in Global ++= Seq(`org.scalatest_scalatest_2.11` % "test" ,
   `org.scalacheck_scalacheck_2.11` % "test" ,
   `org.scala-lang_scala-reflect`,
   `org.scala-lang_scala-library`,
   `org.scala-lang_scala-compiler`,
   `org.neo4j.test_neo4j-harness` % "test" ,
   `org.neo4j_openCypher-frontend-1` % "test" classifier "tests" ,
   `org.neo4j_neo4j-cypher-util-3.4` % "test" classifier "tests" ,
   `org.mockito_mockito-all` % "test" ,
   `org.junit.platform_junit-platform-runner` % "test" ,
   `org.bouncycastle_bctls-jdk15on` % "test" ,
   `org.apache.hadoop_hadoop-minicluster` % "test" ,
   `io.github.nicolasstucki_multisets_2.11` % "test" )



