package org.opencypher.spark.impl.instances.spark

import org.opencypher.spark.impl.spark.SparkCypherEngine

trait SparkCypherInstances {
  implicit val sparkCypherEngineInstance = new SparkCypherEngine
}

