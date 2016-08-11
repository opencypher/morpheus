package org.opencypher.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

object TestSessionFactory {
  lazy val default = create

  def create = SparkSession.builder().master("local[4]").getOrCreate()
}

trait TestSessionSupport extends BeforeAndAfterEach {
  self: FunSuite =>

  implicit val session = TestSessionFactory.default
  implicit val factory = PropertyGraphFactory.create

  override protected def beforeEach(): Unit = {
    factory.clear()
  }
}
