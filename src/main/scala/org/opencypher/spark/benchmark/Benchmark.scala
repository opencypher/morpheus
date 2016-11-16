package org.opencypher.spark.benchmark

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.opencypher.spark.api.CypherResultContainer
import org.opencypher.spark.api.value._
import org.opencypher.spark.impl.{NodeScanIdsSorted, StdPropertyGraph}

object Benchmark {

  import org.neo4j.spark._

  implicit val sparkSession: SparkSession = {
    val conf = new SparkConf(true)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator","org.opencypher.spark.CypherKryoRegistrar")
    conf.set("spark.neo4j.bolt.password", ".")

    //
    // This may or may not help - depending on the query
    // conf.set("spark.kryo.referenceTracking","false")

    //
    // Enable to see if we cover enough
    // conf.set("spark.kryo.registrationRequired", "true")

    //
    // If this is slow, you might be hitting: http://bugs.java.com/view_bug.do?bug_id=8077102
    //
    SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .appName(s"cypher-on-spark-benchmark-${UUID.randomUUID()}")
      .getOrCreate()
  }

  def createGraph() = {
    val neo4j = Neo4j(sparkSession.sparkContext)
    val allNodes = neo4j.cypher("MATCH (a) RETURN a LIMIT 100000").loadNodeRdds.map { row =>
      val node = row(0).asInstanceOf[InternalNode]
      internalNodeToCypherNode(node)
    }

    val allRels = neo4j.cypher("MATCH (:Employee)-[r]->() RETURN r LIMIT 100 UNION ALL MATCH (:Employee)<-[r]-() RETURN r LIMIT 100 ").loadRowRdd.map { row =>
      val relationship = row(0).asInstanceOf[InternalRelationship]
      internalRelationshipToCypherRelationship(relationship)
    }

    val nodes = sparkSession.createDataset(allNodes)(CypherValue.Encoders.cypherNodeEncoder)
    val relationships = sparkSession.createDataset(allRels)(CypherValue.Encoders.cypherRelationshipEncoder)

    new StdPropertyGraph(nodes, relationships)
  }

  def main(args: Array[String]): Unit = {
    val graph = createGraph()

    // warmup
    println("Begin warmup")
    runAndTime(3)(graph.cypher(NodeScanIdsSorted(IndexedSeq("Employee"))))

    println("Begin measurements")
    val times = runAndTime()(graph.cypher(NodeScanIdsSorted(IndexedSeq("Employee"))))

    val avg = times.sum / times.length
    val median = times.sorted.apply(times.length / 2)

    println(s"Min: ${times.min} ms")
    println(s"Max: ${times.max} ms")
    println(s"Avg: $avg ms")
    println(s"Median: $median ms")
  }

  def runAndTime(nbrTimes: Int = 10)(f: => CypherResultContainer): Seq[Long] = {
    (0 until nbrTimes).map { i =>
      println(s"Timing -- Run $i")
      val start = System.currentTimeMillis()
      f.records.exhaust
      val time = System.currentTimeMillis() - start
      println(s"Done -- $time ms")
      time
    }
  }

}

object cypherValue extends (Any => CypherValue) {
  override def apply(v: Any): CypherValue = v match {
    case v: String => CypherString(v)
    case v: java.lang.Byte => CypherInteger(v.toLong)
    case v: java.lang.Short => CypherInteger(v.toLong)
    case v: java.lang.Integer => CypherInteger(v.toLong)
    case v: java.lang.Long => CypherInteger(v)
    case v: java.lang.Float => CypherFloat(v.toDouble)
    case v: java.lang.Double => CypherFloat(v)
    case v: java.lang.Boolean => CypherBoolean(v)
    case v: Array[_] => CypherList(v.map(cypherValue))
    case null => null
    case x => throw new IllegalArgumentException(s"Unexpected property value: $x")
  }

}

object internalNodeToCypherNode extends (InternalNode => CypherNode) {
  import scala.collection.JavaConverters._

  override def apply(michael: InternalNode): CypherNode = {
    val props = michael.asMap().asScala.mapValues(cypherValue)
    val properties = Properties(props.toSeq:_*)
    CypherNode(michael.id(), michael.labels().asScala.toSeq, properties)
  }
}

object internalRelationshipToCypherRelationship extends (InternalRelationship => CypherRelationship) {
  import scala.collection.JavaConverters._

  override def apply(michael: InternalRelationship): CypherRelationship = {
    val props = michael.asMap().asScala.mapValues(cypherValue)
    val properties = Properties(props.toSeq:_*)
    CypherRelationship(michael.id(), michael.startNodeId(), michael.endNodeId(), michael.`type`(), properties)
  }
}
