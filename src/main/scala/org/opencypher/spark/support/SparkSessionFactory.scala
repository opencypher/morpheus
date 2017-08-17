package org.opencypher.spark.support

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.opencypher.spark_legacy.CypherKryoRegistrar
import org.opencypher.spark_legacy.benchmark.Configuration._

import scala.annotation.tailrec

object SparkSessionFactory {

  def createConf = {
    //
    // This may or may not help - depending on the query
    // conf.set("spark.kryo.referenceTracking","false")

    //
    // Enable to see if we cover enough
    // conf.set("spark.kryo.registrationRequired", "true")

    //
    // If this is slow, you might be hitting: http://bugs.java.com/view_bug.do?bug_id=8077102
    //
    val config = new SparkConf(true)
    config.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    config.set("spark.ui.enabled", "false")
    config.set("spark.kryo.registrator", classOf[CypherKryoRegistrar].getCanonicalName)
    config.set("spark.neo4j.bolt.password", Neo4jPassword.get())
    config.set("spark.neo4j.bolt.user", Neo4jUser.get())
    config.set("spark.neo4j.bolt.url", Neo4jAddress.get())
    config
  }


  def getMatchingOrRestart(config: SparkConf): SparkSession =
    SparkSession.synchronized {
      val master = MasterAddress.get()
      lockedGetMatchingOrRestart(config, master)
    }

  @tailrec
  private def lockedGetMatchingOrRestart(config: SparkConf, master: String,
                                         optSession: Option[SparkSession] = None): SparkSession =
    optSession match {
      case Some(session) =>
        val actualMaster = session.sparkContext.master
        if (actualMaster != master) {
          System.err.println(s"Actual master: $actualMaster")
          System.err.println(s"Expected master: $master")
          System.err.println("Stopping spark session for configuration change")
          stop(session)
          lockedGetMatchingOrRestart(config, master, Some(getOrCreate(config, master)))
        } else {
          val actualConf = entries(session.sparkContext.getConf)
          val expectedConf = entries(config)
          if (! expectedConf.forall(actualConf)) {
            System.err.println(s"Actual config entries: $actualConf")
            System.err.println(s"Expected config entries: $expectedConf")
            System.err.println("Stopping spark session for configuration change")
            stop(session)
            lockedGetMatchingOrRestart(config, master, Some(getOrCreate(config, master)))
          }
          else
            session
        }

      case None =>
        lockedGetMatchingOrRestart(config, master, Some(getOrCreate(config, master)))
    }

  private def stop(session: SparkSession) = {
    val context = session.sparkContext
    context.stop()
    while (!context.isStopped) Thread.sleep(1000L)
  }

  private def entries(conf: SparkConf) =
    conf.getAll.toVector.toSet

  private def getOrCreate(config: SparkConf, master: String) = {
    System.err.println("Constructing new spark session")
    System.err.println(config.toDebugString)
    val session = SparkSession
      .builder()
      .config(config)
      .master(master)
      .appName(s"cypher-for-apache-spark-tests-${UUID.randomUUID()}")
      .getOrCreate()
    session.sparkContext.setLogLevel(Logging.get())
    session
  }
}
