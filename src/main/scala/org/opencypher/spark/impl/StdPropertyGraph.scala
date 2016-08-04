package org.opencypher.spark.impl

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType
import org.opencypher.spark._

import scala.collection.immutable.ListMap

object StdPropertyGraph {
  object SupportedQueries {
    val allNodesScan = "MATCH (n) RETURN (n)"
    val allNodesScanProjectAgeName = "MATCH (n) RETURN n.name AS name, n.age AS age"
    val allNodeIds = "MATCH (n) RETURN id(n)"
    val allNodeIdsSortedDesc = "MATCH (n) RETURN id(n) AS id ORDER BY id DESC"
    val getAllRelationshipsOfTypeT = "MATCH ()-[r:T]->() RETURN r"
    val getAllRelationshipsOfTypeTOfLabelA = "MATCH (:A)-[r]->(:B) RETURN r"
    val simpleUnionAll = "MATCH (a:A) RETURN a.name AS name UNION ALL MATCH (b:B) RETURN b.name AS name"
    val simpleUnionDistinct = "MATCH (a:A) RETURN a.name AS name UNION MATCH (b:B) RETURN b.name AS name"
    val optionalMatch = "MATCH (a:A) OPTIONAL MATCH (a)-[r]->(b) RETURN r"
    val unwind = "WITH [1, 2, 3] AS l UNWIND l AS x RETURN x"
    val matchAggregateAndUnwind = "MATCH (a:A) WITH collect(a.name) AS names UNWIND names AS name RETURN name"
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


    case SupportedQueries.optionalMatch =>
      val lhs = nodes.filter(_.labels.contains("A")).map(node => (node.id.v, node))(Encoders.tuple(implicitly[Encoder[Long]], CypherValue.implicits.cypherValueEncoder[CypherNode])).toDF("id_a", "val_a")

      val b = nodes.filter(_.labels.contains("B")).map(node => (node.id.v, node))(Encoders.tuple(implicitly[Encoder[Long]], CypherValue.implicits.cypherValueEncoder[CypherNode])).toDF("id_b", "val_b")
      val rels = relationships.map(rel => (rel.start.v, rel.end.v, rel.id.v, rel))(Encoders.tuple(implicitly[Encoder[Long]], implicitly[Encoder[Long]], implicitly[Encoder[Long]], CypherValue.implicits.cypherValueEncoder[CypherRelationship])).toDF("start_rel", "end_rel", "id_rel", "val_rel")
      val rhs = rels.join(b, functions.expr("id_b = end_rel"))

      val joined = lhs.join(rhs, functions.expr("id_a = start_rel"), "left_outer")

      val rel = joined.select(new Column("val_rel").as("value"))
      val result = rel.as[CypherRelationship](CypherValue.implicits.cypherValueEncoder[CypherRelationship])

      new StdFrame(result.map(r => StdRecord(Array(r), Array.empty)), ListMap("r" -> 0)).result

    case SupportedQueries.simpleUnionAll =>
      val a = nodes.filter(_.labels.contains("A")).map(node => node.properties.getOrElse("name", CypherNull))(CypherValue.implicits.cypherValueEncoder[CypherValue]).toDF("name")
      val b = nodes.filter(_.labels.contains("B")).map(node => node.properties.getOrElse("name", CypherNull))(CypherValue.implicits.cypherValueEncoder[CypherValue]).toDF("name")
      val result = a.union(b).as[CypherValue](CypherValue.implicits.cypherValueEncoder[CypherValue])

      new StdFrame(result.map(v => StdRecord(Array(v), Array.empty)), ListMap("name" -> 0)).result

    case SupportedQueries.simpleUnionDistinct =>
      val a = nodes.filter(_.labels.contains("A")).map(node => node.properties.getOrElse("name", CypherNull))(CypherValue.implicits.cypherValueEncoder[CypherValue]).toDF("name")
      val b = nodes.filter(_.labels.contains("B")).map(node => node.properties.getOrElse("name", CypherNull))(CypherValue.implicits.cypherValueEncoder[CypherValue]).toDF("name")
      val result = a.union(b).distinct().as[CypherValue](CypherValue.implicits.cypherValueEncoder[CypherValue])

      new StdFrame(result.map(v => StdRecord(Array(v), Array.empty)), ListMap("name" -> 0)).result

    case SupportedQueries.unwind =>
      val l = CypherList(Seq(1, 2, 3).map(CypherInteger(_)))
      val start = session.createDataset(Seq(l))(CypherValue.implicits.cypherValueEncoder[CypherList])

      val result = start.flatMap(_.v)(CypherValue.implicits.cypherValueEncoder[CypherValue])

      new StdFrame(result.map(v => StdRecord(Array(v), Array.empty)), ListMap("x" -> 0)).result

    case SupportedQueries.matchAggregateAndUnwind =>
      val lhs = nodes.filter(_.labels.contains("A")).flatMap(_.properties.get("name"))(CypherValue.implicits.cypherValueEncoder[CypherValue])

      val collected = lhs.rdd.aggregate(List.empty[CypherValue])((ls, c) => ls :+ c, _ ++ _)
      val result = session.createDataset(collected)(CypherValue.implicits.cypherValueEncoder[CypherValue])

      new StdFrame(result.map(v => StdRecord(Array(v), Array.empty)), ListMap("name" -> 0)).result


    // *** Functionality left to test

      // [X] Scans (via df access)
      // [X] Projection (via ds.map)
      // [X] Predicate filter (via ds.map to boolean and ds.filter)
      // [X] Expand (via df.join)
      // [X] Union All
      // [X] Union Distinct (spark distinct vs distinct on rdd's with explicit orderability)
      // [X] Optional match (via df.join with type left_outer)
      // [X] UNWIND
      // [X] Bounded var length (via UNION and single steps)
      // [ ] Path
      // [ ] Unbounded var length
      // [ ] Shortest path

      // [X] Aggregation (via rdd, possibly via spark if applicable given the available types)
      // [X] CALL .. YIELD ... (via df.map + procedure registry)
      // [ ] Graph algorithms (map into procedures)

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
