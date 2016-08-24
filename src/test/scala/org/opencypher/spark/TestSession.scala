package org.opencypher.spark

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

object TestSession {

  lazy val default = TestSessionFactory.create


  trait Fixture extends BeforeAndAfterEach {
    self: FunSuite =>

    implicit val session = TestSession.default
    implicit val factory = PropertyGraphFactory.create

    override protected def beforeEach(): Unit = {
      factory.clear()
    }
  }
}

object TestSessionFactory {
  def create = {
    //
    // If this is slow, you might be hitting: http://bugs.java.com/view_bug.do?bug_id=8077102
    //
    SparkSession
      .builder()
      .config(new SparkConf(true))
      .master("local[*]")
      .appName(s"cypher-on-spark-tests-${UUID.randomUUID()}")
      .getOrCreate()
  }
}
