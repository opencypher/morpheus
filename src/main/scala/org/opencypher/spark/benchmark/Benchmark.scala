package org.opencypher.spark.benchmark

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.{Dataset, SparkSession}
import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.neo4j.driver.v1.{AuthToken, AuthTokens, Driver, GraphDatabase}
import org.opencypher.spark.CypherOnSpark
import org.opencypher.spark.api.CypherResultContainer
import org.opencypher.spark.api.value._
import org.opencypher.spark.impl.{NodeScanIdsSorted, SimplePattern, StdPropertyGraph, SupportedQuery}

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
    conf.set("spark.kryo.registrationRequired", "true")

    //
    // If this is slow, you might be hitting: http://bugs.java.com/view_bug.do?bug_id=8077102
    //
    val foo = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .appName(s"cypher-on-spark-benchmark-${UUID.randomUUID()}")
      .getOrCreate()

    foo.sparkContext.setLogLevel("OFF")

    foo
  }

  def createGraph(size: Long) = {
    val neo4j = Neo4j(sparkSession.sparkContext)

    val limit = if (size > 0) s"LIMIT $size"
    val allNodes = neo4j.cypher(s"MATCH (b)-->(a) WITH a, b $limit UNWIND [a, b] AS n RETURN DISTINCT n").loadNodeRdds.map { row =>
      val node = row(0).asInstanceOf[InternalNode]
      internalNodeToCypherNode(node)
    }

    val allRels = neo4j.cypher(s"MATCH ()-[r]->() RETURN r $limit").loadRowRdd.map { row =>
      val relationship = row(0).asInstanceOf[InternalRelationship]
      internalRelationshipToCypherRelationship(relationship)
    }

    val nodes = sparkSession.createDataset(allNodes)(CypherValue.Encoders.cypherNodeEncoder).cache()
    println(s"Finished creating dataset of ${nodes.count()} nodes")
    val relationships = sparkSession.createDataset(allRels)(CypherValue.Encoders.cypherRelationshipEncoder).cache()
    println(s"Finished creating dataset of ${relationships.count()} relationships")

    new StdPropertyGraph(nodes, relationships)
  }

  def benchmarkNeo4j = {
    val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "."))

    val session = driver.session()

//    val query =       """MATCH (n)
//                        |WITH n
//                        |  SKIP 0
//                        |WITH n
//                        |  LIMIT 100000
//                        |WITH id(n) AS id
//                        |WHERE n:Employee
//                        |RETURN id, rand() AS r
//                        |  ORDER BY r DESC""".stripMargin

    val query = """MATCH (a)-[r]->(c)  WITH * SKIP 0 WITH * WHERE a:Employee AND c:Account AND type(r)="HAS_ACCOUNT" WITH * LIMIT 100000 RETURN r""".stripMargin
    val result = session.run(query)

    val plan = result.consume().plan()

    // warmup
    runAndTime(3)(session.run(query))(_.consume())

    // timing
    val times = runAndTime()(session.run(query))(_.consume())

    (times, plan, query)
  }

  def benchmarkWithRdds[T](f: StdPropertyGraph => RDD[T])(graph: StdPropertyGraph) = {
    val result = f(graph)

    // warmup
    runAndTime(3)(f(graph))(_.count)

    val times = runAndTime()(f(graph))(_.count)

    RddResult(times, result.asInstanceOf[RDD[Long]])
  }

  def benchmarkWithDatasets[T](f: StdPropertyGraph => Dataset[T])(graph: StdPropertyGraph) = {
    val plan = f(graph).queryExecution
    val result = f(graph)

    // warmup
    runAndTime(3)(f(graph))(_.count())

    val times = runAndTime()(f(graph))(_.count())

    DatasetsResult(times, plan, result.asInstanceOf[Dataset[Long]])
  }

  def benchmarkCypherOnSpark(query: SupportedQuery)(graph: StdPropertyGraph) = {
    val plan = graph.cypher(query).products.toDS.queryExecution
    val result = graph.cypher(query).products.toDS

    // warmup
    println("Begin warmup")
    runAndTime(3)(graph.cypher(query))(_.products.count())

    println("Begin measurements")
    val times = runAndTime()(graph.cypher(query))(_.products.count())

    CypherOnSparkResult(times, plan, query, result.asInstanceOf[Dataset[Product]])(sparkSession)
  }

  def main(args: Array[String]): Unit = {
    // create a CypherOnSpark graph
    val nbr = 10000
    val graph = createGraph(nbr)
    println("Graph created!")

    val datasets = benchmarkWithDatasets(Dataset.nodeScanIdsSorted("Group", sparkSession))(graph)
    val cypherOnSpark = benchmarkCypherOnSpark(NodeScanIdsSorted(IndexedSeq("Group")))(graph)
    val rdds = benchmarkWithRdds(RDD.nodeScanIdsSorted("Group"))(graph)

//    val datasets = benchmarkWithDatasets(Dataset.nodeScanIdsSorted("Group", sparkSession))(graph)
//    val cypherOnSpark = benchmarkCypherOnSpark(SimplePattern(IndexedSeq("Group"), IndexedSeq("ALLOWED_INHERIT"), IndexedSeq("Company")))(graph)
//    val rdds = benchmarkWithRdds(RDD.nodeScanIdsSorted("Group"))(graph)

    println(s"We limited ourselves to ${if(nbr < 0) "ALL" else nbr} nodes and ${if(nbr < 0) "ALL" else nbr} relationships")

    //    printSummary(cypherOnSpark)
    //    printSummary(datasets)
    //    printSummary(rdds)
    compare(cypherOnSpark, datasets, rdds)
  }

  def compare[T](results: Result*) = {
    results.foreach { r =>
      println(s"$r: ${r.count} rows")
      println(s"$r: ${r.hash} hashed")
      println(s"$r: ${r.avg} ms")
    }
  }

  def printSummary[T](result: Result) = {
    val times = result.times
    val plan = result.plan
    val query = result.query

    val median = times.sorted.apply(times.length / 2)

    println(s"\nQuery plan:\n $plan")
    println("=================================")
    println(s"Finished benchmarking of \n\t$query")
    println(s"number of returned rows: $result")

    println(s"Min: ${times.min} ms")
    println(s"Max: ${times.max} ms")
    println(s"Avg: ${result.avg} ms")
    println(s"Median: $median ms")
  }

  def runAndTime[T, U](nbrTimes: Int = 10)(f: => T)(drainer: T => U): Seq[Long] = {
    (0 until nbrTimes).map { i =>
      println(s"Timing -- Run $i")
      val start = System.currentTimeMillis()
      drainer(f)
      val time = System.currentTimeMillis() - start
      println(s"Done -- $time ms")
      time
    }
  }

}

sealed trait Result {
  def times: Seq[Long]
  def plan: String
  def query: String
//  def result: RDD[Long]
  def count: Long
  def hash: Long

  def avg = times.sum / times.length
}

case class CypherOnSparkResult(times: Seq[Long], _plan: QueryExecution, _query: SupportedQuery, _result: Dataset[Product])(sparkSession: SparkSession) extends Result {

  import sparkSession.implicits._

  override def plan: String = _plan.toString()

  override def query: String = _query.toString

//  override def result: RDD[Long] = _result.map(_.productElement(0).asInstanceOf[Long]).rdd

  override def toString: String = "CypherOnSpark"

  override def count: Long = _result.count()

  override def hash: Long = _result.map(_.productElement(0).asInstanceOf[Long]).reduce {
    (acc, next) => acc.hashCode() + next.hashCode()
  }
}

case class DatasetsResult(times: Seq[Long], _plan: QueryExecution, _result: Dataset[Long]) extends Result {
  override def plan: String = _plan.toString()

  override def query: String = "DATASET BENCHMARK"

//  override def result: RDD[Long] = _result.rdd

  override def toString: String = "Datasets"

  override def count: Long = _result.count()

  override def hash: Long = _result.reduce {
    (acc, next) => acc.hashCode() + next.hashCode()
  }
}

case class RddResult[T](times: Seq[Long], result: RDD[Long]) extends Result {
  override def plan: String = "no plan for rdds"

  override def query: String = "RDD BENCHMARK"

  override def toString: String = "RDDs"

  override def count: Long = result.count()

  override def hash: Long = result.reduce {
    (acc, next) => acc.hashCode() + next.hashCode()
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
    CypherNode(michael.id(), michael.labels().asScala.toArray, properties)
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
