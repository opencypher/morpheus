package org.opencypher.spark.impl

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.spark._

import scala.collection.immutable.ListMap

object StdPropertyGraph {
  object SupportedQueries {
    val allNodesScan = "MATCH (n) RETURN (n)"
    val allNodesScanProjectAgeName = "MATCH (n) RETURN n.name AS name, n.age AS age"
    val allNodeIds = "MATCH (n) RETURN id(n)"
    val allNodeIdsSortedDesc = "MATCH (n) RETURN id(n) AS id ORDER BY id DESC"
    val getAllRelationshipsOfTypeT = "MATCH ()-[r:T]->() RETURN r"
    val getAllRelationshipsOfTypeTOfLabelA = "MATCH (:A)-[r:T]->(:B) RETURN r"
  }
}
abstract class StdPropertyGraph(implicit private val session: SparkSession) extends PropertyGraph {

  import StdPropertyGraph.SupportedQueries
  import StdRecord.implicits._
  import session.implicits._

  override def cypher(query: String) = query match {

    case SupportedQueries.allNodesScan =>
      new StdFrame(nodes.map[StdRecord] { node: CypherNode =>
        StdRecord(Array(node), Array(node.id.v))
      }, Map("value" -> 0)).result

    case SupportedQueries.allNodesScanProjectAgeName =>
      new StdFrame(nodes.map[StdRecord] { node: CypherNode =>
        StdRecord(Array(node.properties("name"), node.properties("age")), Array.empty)
      }, Map("name" -> 0, "age" ->1)).result

    case SupportedQueries.allNodeIds =>
      new StdFrame(nodes.map[StdRecord] { node: CypherNode =>
        StdRecord(Array(CypherInteger(node.id.v)), Array.empty)
      }, ListMap("value" -> 0)).result

    case SupportedQueries.allNodeIdsSortedDesc =>
      new StdFrame(session.createDataset(nodes.map[StdRecord] { node: CypherNode =>
        StdRecord(Array(CypherInteger(node.id.v)), Array.empty)
      }.rdd.sortBy[Long]({ record =>
        record.values(0).asInstanceOf[CypherInteger].v
      }, false)), ListMap("value" -> 0)).result

    case SupportedQueries.getAllRelationshipsOfTypeT =>
      new StdFrame(relationships.filter(_.typ == "T").map(r => StdRecord(Array(r), Array.empty)), ListMap("r" -> 0)).result

    case SupportedQueries.getAllRelationshipsOfTypeTOfLabelA =>
      val a = nodes.filter(_.labels.contains("A")).map(node => (node.id.v, node))(Encoders.tuple(implicitly[Encoder[Long]], CypherValue.implicits.cypherValueEncoder[CypherNode])).toDF("id_a", "val_a")
      val b = nodes.filter(_.labels.contains("B")).map(node => (node.id.v, node))(Encoders.tuple(implicitly[Encoder[Long]], CypherValue.implicits.cypherValueEncoder[CypherNode])).toDF("id_b", "val_b")
      val rels = relationships.map(rel => (rel.start.v, rel.end.v, rel.id.v, rel))(Encoders.tuple(implicitly[Encoder[Long]], implicitly[Encoder[Long]], implicitly[Encoder[Long]], CypherValue.implicits.cypherValueEncoder[CypherRelationship])).toDF("start_rel", "end_rel", "id_rel", "val_rel")
      val joined =
        rels
          .join(a, functions.expr("id_a = start_rel"))
          .join(b, functions.expr("id_b = end_rel"))
          .select(new Column("val_rel").as("value"))
      val result = joined.as[CypherRelationship](CypherValue.implicits.cypherValueEncoder[CypherRelationship])

      new StdFrame(result.map(r => StdRecord(Array(r), Array.empty)), ListMap("r" -> 0)).result

      // *** Functionality left to test

      // Union
      // Optional match
      // UNWIND
      // Path
      // [X] Bounded var length
      // Unbounded var length
      // Shortest path

      // [X] Aggregation

      // [X] CALL .. YIELD ...

      // [X] Graph algorithms

      /* Updates

        ... 2 3 4 2 2 ... => MERGE (a:A {id: id ...}

        ... | MERGE 2
        ... | MERGE 3
        ... | MERGE 4
        ... | MERGE 2
        ... | MERGE 2

        ... | CREATE-MERGE 2
        ... | MATCH-MERGE 3
        ... | CREATE-MERGE 4
        ... | CREATE-MERGE 2
        ... | CREATE-MERGE 2

     */

      // CypherFrames and expression evaluation (including null issues)

    case _ =>
      ???
  }
}

class StdFrame(ds: Dataset[StdRecord], columns: Map[String, Int])(implicit session: SparkSession) {
  def result: CypherResult = new StdCypherResult(ds, columns)
}

class StdCypherResult(ds: Dataset[StdRecord], columns: Map[String, Int])(implicit session: SparkSession) extends CypherResult with Serializable {

  import session.implicits._

  override def toDF: DataFrame =  {
    ds.toDF()
////    val u = ds.toDF().map { (row: Row) => row }
//
////    session.
////    ds.map(cypherRecord).toDF.select(functions.expr("value.name"), functions.expr("value.age"))
//    val schema = StructType(columns.keys.map { k => StructField(k, BinaryType, nullable = true) }.toSeq)
//    val df = ds.map(stdRecord => Row(cypherRecord(stdRecord).values.toSeq: _*))(RowEncoder(schema))
//    val alt = ds.toDF()
//    df
  }

  override def toDS[T : Encoder](f: CypherRecord => T): Dataset[T] =
    ds.map { stdRecord => f(cypherRecord(stdRecord)) }

  private def cypherRecord(stdRecord: StdRecord) =
    columns.mapValues { idx => stdRecord.values(idx) }

  override def show(): Unit = toDS { _.toString }.show(false)
}
