package org.opencypher.spark

import org.apache.spark.sql._

abstract class PropertyGraphImpl(sc: SQLContext) extends PropertyGraph with CypherEncoders {

  import PropertyGraphImpl.SupportedQueries

  override def cypher(query: String) = query match {
    case SupportedQueries.allNodesScan =>
      new CypherFrame(nodes.map[CypherRecord] { node: CypherNode =>
        CypherRecord(Array(node), Array(node.id))
      }, Map("value" -> 0)).result

    case SupportedQueries.allNodeIds =>
      new CypherFrame(nodes.map[CypherRecord] { node: CypherNode =>
        CypherRecord(Array(CypherInteger(node.id)), Array.empty)
      }, Map("value" -> 0)).result

    case SupportedQueries.allNodeIdsSortedDesc =>
      new CypherFrame(sc.createDataset(nodes.map[CypherRecord] { node: CypherNode =>
        CypherRecord(Array(CypherInteger(node.id)), Array.empty)
      }.rdd.sortBy[Long]({ record =>
        record.values(0).asInstanceOf[CypherInteger].v
      }, false)), Map("value" -> 0)).result

    case _ =>
      ???
  }
}

class CypherFrame(df: Dataset[CypherRecord], columns: Map[String, Int]) {
  def result: CypherResult = new CypherResultImpl(df, columns)
}

class CypherResultImpl(df: Dataset[CypherRecord], columns: Map[String, Int]) extends CypherResult with Serializable {
  override def toDF: DataFrame = df.toDF()

  override def toDS[T : Encoder](f: (Map[String, CypherValue]) => T): Dataset[T] = {
    df.map { r: CypherRecord  =>
      f(columns.mapValues(idx => r.values(idx)))
    }
  }

  override def show(): Unit = df.show()
}

object PropertyGraphImpl {
  object SupportedQueries {
    val allNodesScan = "/* All node scan */ MATCH (n) RETURN (n)"
    val allNodeIds = "/* All node scan */ MATCH (n) RETURN id(n)"
    val allNodeIdsSortedDesc = "/* All node scan */ MATCH (n) RETURN id(n) AS id ORDER BY id DESC"
  }
}
