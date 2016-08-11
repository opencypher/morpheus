package org.opencypher.spark

import org.apache.spark.sql.SparkSession
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
  def create = SparkSession.builder().master("local[4]").getOrCreate()
}
