package org.opencypher.spark.impl

import org.apache.spark.sql._
import org.opencypher.spark.api.{CypherResultContainer, PropertyGraph}
import org.opencypher.spark.api.value.{CypherNode, CypherRelationship}
import org.opencypher.spark.impl.frame.{Collect, Desc, SortItem}
import org.opencypher.spark.impl.util.SlotSymbolGenerator

import scala.language.implicitConversions

object StdPropertyGraph {

  object SupportedQueries {
    val allNodesScan = "MATCH (n) RETURN (n)"
    val allNodesScanProjectAgeName = "MATCH (n) RETURN n.name, n.age"
    val allNodeIds = "MATCH (n) RETURN id(n)"
    val allNodeIdsSortedDesc = "MATCH (n) RETURN id(n) AS id ORDER BY id DESC"
    val getAllRelationshipsOfTypeT = "MATCH ()-[r:T]->() RETURN r"
    val getAllRelationshipsOfTypeTOfLabelA = "MATCH (:A)-[r]->(:B) RETURN r"
    val simpleUnionAll = "MATCH (a:A) RETURN a.name AS name UNION ALL MATCH (b:B) RETURN b.name AS name"
    val simpleUnionDistinct = "MATCH (a:A) RETURN a.name AS name UNION MATCH (b:B) RETURN b.name AS name"
    val optionalMatch = "MATCH (a:A) OPTIONAL MATCH (a)-[r]->(b) RETURN r"
    val unwind = "WITH [1, 2, 3] AS l UNWIND l AS x RETURN x"
    val matchAggregate = "MATCH (a:A) RETURN collect(a.name) AS names"
    val matchAggregateAndUnwind = "MATCH (a:A) WITH collect(a.name) AS names UNWIND names AS name RETURN name"
    val shortestPath = "MATCH (a {name: 'Ava'}), (b {name: 'Sasha'}) MATCH p=shortestPath((a)-->(b)) RETURN p"
    // not implemented
    val boundVarLength = "MATCH (a:A)-[r*2]->(b:B) RETURN r"
  }
}

class StdPropertyGraph(val nodes: Dataset[CypherNode], val relationships: Dataset[CypherRelationship])
                      (implicit private val session: SparkSession) extends PropertyGraph {

  import StdPropertyGraph.SupportedQueries

  override def cypher(query: String): CypherResultContainer = {
    implicit val planningContext = new PlanningContext(new SlotSymbolGenerator, nodes, relationships)
    implicit val runtimeContext = new StdRuntimeContext(session)


    val frames = new FrameProducer
    import frames._

    query match {

      case SupportedQueries.allNodesScan =>
        StdCypherResultContainer.fromProducts(allNodes('n).asProduct)

      case SupportedQueries.allNodesScanProjectAgeName =>
        val productFrame = allNodes('n).asProduct
        val withName = productFrame.propertyValue('n, 'name)(Symbol("n.name"))
        val withAge = withName.propertyValue('n, 'age)(Symbol("n.age"))
        val selectFields = withAge.selectFields(Symbol("n.name"), Symbol("n.age"))

        StdCypherResultContainer.fromProducts(selectFields)

      case SupportedQueries.getAllRelationshipsOfTypeTOfLabelA =>
        val aAsProduct = allNodes('a).labelFilter("A").asProduct
        val aWithId = aAsProduct.nodeId('a)(Symbol("id(a)"))

        val bAsProduct = allNodes('b).labelFilter("B").asProduct
        val bWithId = bAsProduct.nodeId('b)(Symbol("id(b)"))

        val rAsProduct = allRelationships('r).asProduct
        val rWithStartId = rAsProduct.relationshipStartId('r)(Symbol("startId(r)"))
        val rWithStartAndEndId = rWithStartId.relationshipEndId('r)(Symbol("endId(r)"))
        val relsAsRows = rWithStartAndEndId.asRow

        val joinRelA = relsAsRows.join(aWithId.asRow).on(rWithStartId.projectedField.sym)(aWithId.projectedField.sym)
        val joinRelB = joinRelA.join(bWithId.asRow).on(rWithStartAndEndId.projectedField.sym)(bWithId.projectedField.sym)

        val selectField = joinRelB.asProduct.selectFields('r)

        StdCypherResultContainer.fromProducts(selectField)

      case SupportedQueries.simpleUnionAll =>
        val aAsProduct = allNodes('a).labelFilter("A").asProduct
        val aNames = aAsProduct.propertyValue('a, 'name)(Symbol("a.name"))
        val aNameRenamed = aNames.aliasField(Symbol("a.name") -> 'name)
        val selectFieldA = aNameRenamed.selectFields('name)

        val allNodesB = allNodes('b)
        val bAsProduct = allNodesB.labelFilter("B").asProduct
        val bNames = bAsProduct.propertyValue('b, 'name)(Symbol("b.name"))
        val bNameRenamed = bNames.aliasField(Symbol("b.name") -> 'name)
        val selectFieldB = bNameRenamed.selectFields('name)

        val union = selectFieldA.unionAll(selectFieldB)

        StdCypherResultContainer.fromProducts(union)

      case SupportedQueries.allNodeIdsSortedDesc =>
        val nodeWithId = allNodes('n).asProduct.nodeId('n)('nid)

        val sorted = nodeWithId.orderBy(SortItem('nid, Desc)).selectFields('nid).aliasField('nid -> 'id)

        StdCypherResultContainer.fromProducts(sorted)

      case SupportedQueries.matchAggregate =>
        val nodesWithProperty = allNodes('a).labelFilter("A").asProduct.propertyValue('a, 'name)('name)

        val grouped = nodesWithProperty.groupBy()(Collect('name)('names))

        StdCypherResultContainer.fromProducts(grouped)

      case SupportedQueries.matchAggregateAndUnwind =>
        val nodesWithProperty = allNodes('a).labelFilter("A").asProduct.propertyValue('a, 'name)('name)

        val grouped = nodesWithProperty.groupBy()(Collect('name)('names))
        val unwindedAndSelected = grouped.unwind('names, 'name).selectFields('name)

        StdCypherResultContainer.fromProducts(unwindedAndSelected)

      case SupportedQueries.getAllRelationshipsOfTypeT =>
        val relationships = allRelationships('r).typeFilter("T").asRow

        StdCypherResultContainer.fromRows(relationships)


//      case SupportedQueries.getAllRelationshipsOfTypeT =>
//        new StdFrame(relationships.filter(_.typ == "T").map(r => StdRecord(Array(r), Array.empty)), ListMap("r" -> 0)).result
//
//
//
//      case SupportedQueries.optionalMatch =>
//        val lhs = nodes.filter(_.labels.contains("A")).map(node => (node.id.v, node))(Encoders.tuple(implicitly[Encoder[Long]], CypherValue.implicits.cypherValueEncoder[CypherNode])).toDF("id_a", "val_a")
//
//        val b = nodes.filter(_.labels.contains("B")).map(node => (node.id.v, node))(Encoders.tuple(implicitly[Encoder[Long]], CypherValue.implicits.cypherValueEncoder[CypherNode])).toDF("id_b", "val_b")
//        val rels = relationships.map(rel => (rel.start.v, rel.end.v, rel.id.v, rel))(Encoders.tuple(implicitly[Encoder[Long]], implicitly[Encoder[Long]], implicitly[Encoder[Long]], CypherValue.implicits.cypherValueEncoder[CypherRelationship])).toDF("start_rel", "end_rel", "id_rel", "val_rel")
//        val rhs = rels.join(b, functions.expr("id_b = end_rel"))
//
//        val joined = lhs.join(rhs, functions.expr("id_a = start_rel"), "left_outer")
//
//        val rel = joined.select(new Column("val_rel").as("value"))
//        val result = rel.as[CypherRelationship](CypherValue.implicits.cypherValueEncoder[CypherRelationship])
//
//        new StdFrame(result.map(r => StdRecord(Array(r), Array.empty)), ListMap("r" -> 0)).result
//
//      case SupportedQueries.simpleUnionDistinct =>
//        val a = nodes.filter(_.labels.contains("A")).map(node => node.properties.getOrElse("name", CypherNull))(CypherValue.implicits.cypherValueEncoder[CypherValue]).toDF("name")
//        val b = nodes.filter(_.labels.contains("B")).map(node => node.properties.getOrElse("name", CypherNull))(CypherValue.implicits.cypherValueEncoder[CypherValue]).toDF("name")
//        val result = a.union(b).distinct().as[CypherValue](CypherValue.implicits.cypherValueEncoder[CypherValue])
//
//        new StdFrame(result.map(v => StdRecord(Array(v), Array.empty)), ListMap("name" -> 0)).result
//
//      case SupportedQueries.unwind =>
//        val l = CypherList(Seq(1, 2, 3).map(CypherInteger(_)))
//        val start = session.createDataset(Seq(l))(CypherValue.implicits.cypherValueEncoder[CypherList])
//
//        val result = start.flatMap(_.v)(CypherValue.implicits.cypherValueEncoder[CypherValue])
//
//        new StdFrame(result.map(v => StdRecord(Array(v), Array.empty)), ListMap("x" -> 0)).result
//
//
//      case SupportedQueries.shortestPath =>
//        val a = nodes.flatMap(_.properties.get("name").filter(_ == "Ava"))(CypherValue.implicits.cypherValueEncoder[CypherValue])
//        val b = nodes.flatMap(_.properties.get("name").filter(_ == "Sasha"))(CypherValue.implicits.cypherValueEncoder[CypherValue])
//
//        ???
//
//      case SupportedQueries.boundVarLength =>
//        val a = nodes.filter(_.labels.contains("A")).map(node => (node.id.v, node))(Encoders.tuple(implicitly[Encoder[Long]], CypherValue.implicits.cypherValueEncoder[CypherNode])).toDF("id_a", "val_a")
//        val b = nodes.filter(_.labels.contains("B")).map(node => (node.id.v, node))(Encoders.tuple(implicitly[Encoder[Long]], CypherValue.implicits.cypherValueEncoder[CypherNode])).toDF("id_b", "val_b")
//        val rels1 = relationships.map(rel => (rel.start.v, rel.end.v, rel.id.v, rel))(Encoders.tuple(implicitly[Encoder[Long]], implicitly[Encoder[Long]], implicitly[Encoder[Long]], CypherValue.implicits.cypherValueEncoder[CypherRelationship])).toDF(
//          "start_rel1", "end_rel1", "id_rel1", "val_rel1"
//        )
//        val rels2 = rels1.select(
//          new Column("start_rel1").as("start_rel2"),
//          new Column("end_rel1").as("end_rel2"),
//          new Column("id_rel1").as("id_rel2"),
//          new Column("val_rel1").as("val_rel2")
//        )
//
//        val step1out = rels1.join(a, functions.expr("id_a = start_rel1"))
//        val step1done = step1out.join(b, functions.expr("end_rel1 = id_b"))
//
//        val prepare1 = step1done.select(new Column("val_rel1").as("r"))
//        val result1 = prepare1
//          .as[CypherRelationship](CypherValue.implicits.cypherValueEncoder[CypherRelationship])
//          .map(r => CypherList(Seq(r)))(CypherValue.implicits.cypherValueEncoder[CypherList])
//
//        val step2out = step1out.join(rels2, functions.expr("end_rel1 = start_rel2"))
//        val step2done = step2out.join(b, functions.expr("end_rel2 = id_b"))
//
//        val prepare2 = step2done.select(new Column("val_rel1").as("r1"), new Column("val_rel2").as("r2"))
//        val encoder2 = ExpressionEncoder.tuple(Seq(CypherValue.implicits.cypherValueEncoder[CypherRelationship], CypherValue.implicits.cypherValueEncoder[CypherRelationship]).map(_.asInstanceOf[ExpressionEncoder[_]])).asInstanceOf[Encoder[Product]]
//        val result2 = prepare2
//          .as[Product](encoder2)
//          .map(p => CypherList(p.productIterator.map(_.asInstanceOf[CypherRelationship]).toList))(CypherValue.implicits.cypherValueEncoder[CypherList])
//
//        val result = result1.union(result2)
//
//        new StdFrame(result.map(r => StdRecord(Array(r), Array.empty)), ListMap("r" -> 0)).result
//
//      // *** Functionality left to test
//
//      // [X] Scans (via df access)
//      // [X] Projection (via ds.map)
//      // [X] Predicate filter (via ds.map to boolean and ds.filter)
//      // [X] Expand (via df.join)
//      // [X] Union All
//      // [X] Union Distinct (spark distinct vs distinct on rdd's with explicit orderability)
//      // [X] Optional match (via df.join with type left_outer)
//      // [X] UNWIND
//      // [X] Bounded var length (via UNION and single steps)
//      // [X] Unbounded var length (we think we can do it in a loop but it will be probably be really expensive)
//      // [-] Shortest path -- not easily on datasets/dataframes directly but possible via graphx
//
//      // [X] Aggregation (via rdd, possibly via spark if applicable given the available types)
//      // [X] CALL .. YIELD ... (via df.map + procedure registry)
//      // [X] Graph algorithms (map into procedures)
//
//      /* Updates
//
//        ... 2 3 4 2 2 ... => MERGE (a:A {id: id ...}
//
//        ... | MERGE 2
//        ... | MERGE 3
//        ... | MERGE 4
//        ... | MERGE 2
//        ... | MERGE 2
//
//        ... | CREATE-MERGE 2
//        ... | MATCH-MERGE 3
//        ... | CREATE-MERGE 4
//        ... | CREATE-MERGE 2
//        ... | CREATE-MERGE 2
//
//     */
//
//      // CypherFrames and expression evaluation (including null issues)
      case _ =>
        throw new UnsupportedOperationException("I don't want a NotImplemented warning")
    }
  }
}
