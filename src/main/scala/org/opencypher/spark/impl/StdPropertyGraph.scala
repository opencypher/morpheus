package org.opencypher.spark.impl

import org.apache.spark.sql._
import org.opencypher.spark._

import scala.collection.immutable.ListMap

object StdPropertyGraph {
  object SupportedQueries {
    val allNodesScan = "/* all node scan */ MATCH (n) RETURN (n)"
    val allNodeIds = "/* all node ids */ MATCH (n) RETURN id(n)"
    val allNodeIdsSortedDesc = "/* all node ids sorted desc */ MATCH (n) RETURN id(n) AS id ORDER BY id DESC"
  }
}
abstract class StdPropertyGraph(sc: SQLContext) extends PropertyGraph  {

  import StdRecord.implicits._
  import StdPropertyGraph.SupportedQueries

  override def cypher(query: String) = query match {
    case SupportedQueries.allNodesScan =>
      new StdFrame(nodes.map[StdRecord] { node: CypherNode =>
        StdRecord(Array(node), Array(node.id))
      }, Map("value" -> 0)).result

    case SupportedQueries.allNodeIds =>
      new StdFrame(nodes.map[StdRecord] { node: CypherNode =>
        StdRecord(Array(CypherInteger(node.id)), Array.empty)
      }, ListMap("value" -> 0)).result

    case SupportedQueries.allNodeIdsSortedDesc =>
      new StdFrame(sc.createDataset(nodes.map[StdRecord] { node: CypherNode =>
        StdRecord(Array(CypherInteger(node.id)), Array.empty)
      }.rdd.sortBy[Long]({ record =>
        record.values(0).asInstanceOf[CypherInteger].v
      }, false)), ListMap("value" -> 0)).result

    case _ =>
      ???
  }
}

class StdFrame(df: Dataset[StdRecord], columns: Map[String, Int]) {
  def result: CypherResult = new StdCypherResult(df, columns)
}

class StdCypherResult(df: Dataset[StdRecord], columns: Map[String, Int]) extends CypherResult with Serializable {
  override def toDF: DataFrame = df.toDF()

  override def toDS[T : Encoder](f: CypherRecord => T): Dataset[T] = {
    df.map { r: StdRecord  =>
      f(columns.mapValues(idx => r.values(idx)))
    }
  }

  override def show(): Unit = df.show()
}
