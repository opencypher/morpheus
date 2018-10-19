package org.opencypher.spark.examples

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph.{GraphName, Node}
import org.opencypher.spark.api.io.util.HiveTableName
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.util.ConsoleApp

object HiveSupportExample extends ConsoleApp {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .enableHiveSupport()
    .appName(s"caps-local-${UUID.randomUUID()}")
    .getOrCreate()
    sparkSession.sparkContext.setLogLevel("error")
  implicit val session = CAPSSession.create(sparkSession)

  val hiveDatabaseName = "socialNetwork"
  session.sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $hiveDatabaseName")


  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)
  val tmp = s"file:///${System.getProperty("java.io.tmpdir").replace("\\", "/")}/${System.currentTimeMillis()}"

  val fs = GraphSources.fs(tmp, Some(hiveDatabaseName)).parquet
  val graphName = GraphName("sn")

  fs.store(graphName, socialNetwork)

  val nodeTableName = HiveTableName(hiveDatabaseName, graphName, Node, Set("Person"))
  val nodeScan = session.sparkSession.sql(s"SELECT * FROM $nodeTableName WHERE property_age >= 15")

  // TODO: do stuff

  fs.delete(graphName)
  session.sparkSession.sql(s"DROP DATABASE IF EXISTS $hiveDatabaseName CASCADE")
}
