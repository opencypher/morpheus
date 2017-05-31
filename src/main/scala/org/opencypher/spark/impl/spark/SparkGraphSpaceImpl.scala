package org.opencypher.spark.impl.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkGraphSpace}

class SparkGraphSpaceImpl(val session: SparkSession) extends SparkGraphSpace {
  private var _globals: GlobalsRegistry = GlobalsRegistry.none

  def globals = _globals

  override def base: SparkCypherGraph = ???
}
