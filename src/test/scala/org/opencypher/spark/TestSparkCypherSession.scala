package org.opencypher.spark

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.spark.SparkCypherSession
import org.opencypher.spark_legacy.{CypherKryoRegistrar, PropertyGraphFactory}
import org.opencypher.spark_legacy.benchmark.Configuration.{Logging, Neo4jAddress, Neo4jPassword, Neo4jUser}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

object TestSparkCypherSession {

  lazy val default = SparkCypherSession.create(TestSparkSessionFactory.create)


  trait Fixture extends BeforeAndAfterEach {
    self: FunSuite =>

    implicit val session = TestSparkCypherSession.default
    implicit val sparkSession = session.sparkSession
    implicit val factory = PropertyGraphFactory.create

    override protected def beforeEach(): Unit = {
      factory.clear()
    }
  }
}

object TestSparkSessionFactory {
  def create = {
    val conf = new SparkConf(true)
    conf.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    conf.set("spark.kryo.registrator", classOf[CypherKryoRegistrar].getCanonicalName)
    conf.set("spark.neo4j.bolt.password", Neo4jPassword.get())
    conf.set("spark.neo4j.bolt.user", Neo4jUser.get())
    conf.set("spark.neo4j.bolt.url", Neo4jAddress.get())

    //
    // This may or may not help - depending on the query
    // conf.set("spark.kryo.referenceTracking","false")

    //
    // Enable to see if we cover enough
    // conf.set("spark.kryo.registrationRequired", "true")

    //
    // If this is slow, you might be hitting: http://bugs.java.com/view_bug.do?bug_id=8077102
    //
    val session = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .appName(s"cypher-for-apache-spark-tests-${UUID.randomUUID()}")
      .getOrCreate()

    session.sparkContext.setLogLevel(Logging.get())
    session
  }
}
