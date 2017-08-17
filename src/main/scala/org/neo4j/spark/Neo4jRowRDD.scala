package org.neo4j.spark

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.neo4j.driver.v1._

import scala.collection.JavaConverters._

class Neo4jRowRDD(@transient sc: SparkContext, val query: String, val parameters: Seq[(String,Any)])
  extends RDD[Row](sc, Nil) {

  private val config = Neo4jConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val driver = config.driver()
    val session = driver.session()

    try {
      val result: StatementResult = session.run(query, parameters.toMap.mapValues(_.asInstanceOf[AnyRef]).asJava)

      result.asScala.map((record) => {
        val keyCount = record.size()

        val res = if (keyCount == 0) Row.empty
        else if (keyCount == 1) Row(record.get(0).asObject())
        else {
          val builder = Seq.newBuilder[AnyRef]
          var i = 0
          while (i < keyCount) {
            builder += record.get(i).asObject()
            i = i + 1
          }
          Row.fromSeq(builder.result())
        }
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

object Neo4jRowRDD {
  def apply(sc: SparkContext, query: String, parameters:Seq[(String,Any)] = Seq.empty) = new Neo4jRowRDD(sc, query, parameters)
}
