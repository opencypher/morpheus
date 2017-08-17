package org.neo4j.spark

import java.util
import java.util.{Collections, NoSuchElementException}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.neo4j.driver.v1.{Driver, Value, GraphDatabase, Values}

import scala.collection.JavaConverters._

class Neo4jTupleRDD(@transient sc: SparkContext, val query: String, val parameters: Seq[(String, AnyRef)])
  extends RDD[Seq[(String, AnyRef)]](sc, Nil) {

  private val config = Neo4jConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[Seq[(String, AnyRef)]] = {
    val driver: Driver = config.driver()
    val session = driver.session()
    try {
      val result = session.run(query, parameters.toMap.asJava)

      result.asScala.map((record) => {
        val res = record.asMap().asScala.toSeq
        if (!result.hasNext) {
          if (session.isOpen) session.close()
          driver.close()
        }
        res
      })
    } finally {
      if (session.isOpen) session.close()
      driver.close()
    }
  }

  override protected def getPartitions: Array[Partition] = Array(new Neo4jPartition())
}

object Neo4jTupleRDD {
  def apply(sc: SparkContext, query: String, parameters: Seq[(String,AnyRef)] = Seq.empty) = new Neo4jTupleRDD(sc, query, parameters)
}


