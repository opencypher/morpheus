package org.opencypher.spark.benchmark

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}
import org.opencypher.spark.api.value._
import org.opencypher.spark.benchmark.Converters.{internalNodeToAccessControlNode, internalNodeToCypherNode, internalRelToAccessControlRel, internalRelationshipToCypherRelationship}
import org.opencypher.spark.impl._

object Benchmark {

  implicit var sparkSession: SparkSession = _

  def init() = {
    val conf = new SparkConf(true)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "org.opencypher.spark.CypherKryoRegistrar")
    conf.set("spark.neo4j.bolt.password", ".")
    conf.set("spark.driver.memory", "471859200")
    // Enable to see if we cover enough
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.default.parallelism", Parallelism.get())

    //
    // This may or may not help - depending on the query
    // conf.set("spark.kryo.referenceTracking","false")
    //

    val builder = SparkSession.builder().config(conf)
    if (MasterAddress.get().nonEmpty)
      sparkSession = SparkSession.builder().config(conf).master(MasterAddress.get()).getOrCreate()
    else {
      //
      // If this is slow, you might be hitting: http://bugs.java.com/view_bug.do?bug_id=8077102
      //
      sparkSession = builder.master("local[*]").appName(s"cypher-on-spark-benchmark-${UUID.randomUUID()}").getOrCreate()
    }
    sparkSession.sparkContext.setLogLevel(Logging.get())

    sparkSession
  }

  def loadRDDs() = {
    val nodeRDD = sparkSession.sparkContext.objectFile[CypherNode](NodeFilePath.get())
    val relsRDD = sparkSession.sparkContext.objectFile[CypherRelationship](RelFilePath.get())

    Tests.printSums(nodeRDD, relsRDD)

    nodeRDD -> relsRDD
  }

  def createGraph(size: Long) = {
    val (allNodes, allRels) = loadRDDs()
    println("Nodes and relationships read from disk")

    val defaultParallelism: Int = sparkSession.sparkContext.defaultParallelism
    println(s"Parallelism: $defaultParallelism")

    val nodes = sparkSession.createDataset(allNodes)(CypherValue.Encoders.cypherNodeEncoder).limit(size.toInt).cache().repartition(defaultParallelism).cache()
    println(s"Finished creating dataset of ${nodes.count()} nodes")
    val relationships = sparkSession.createDataset(allRels)(CypherValue.Encoders.cypherRelationshipEncoder).limit(size.toInt).cache().repartition(defaultParallelism).cache()
    println(s"Finished creating dataset of ${relationships.count()} relationships")

    new StdPropertyGraph(nodes, relationships)
  }

  def benchmarkNeo4j(query: String) = {
    import scala.collection.JavaConverters._

    val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "."))

    val session = driver.session()

    val result = session.run(query)
    val list = result.list()
    val count = list.size()
    val checksum = list.asScala.map(_.get(0).hashCode()).sum

    val plan = session.run(s"EXPLAIN $query").consume().plan()

    // warmup
    runAndTime(3)(session.run(query))(_.consume())

    // timing
    val times = runAndTime()(session.run(query))(_.consume())

    Neo4jResult(times, count, checksum, plan.toString, query)
  }

  def benchmarkWithRdds[T](f: StdPropertyGraph => RDD[T])(graph: StdPropertyGraph) = {
    val count = f(graph).count()
    val sum = f(graph).map(_.hashCode()).reduce(_ + _)

    // warmup
    runAndTime(3)(f(graph))(_.count)

    val times = runAndTime()(f(graph))(_.count)

    RddResult(times, count, sum)
  }

  def benchmarkWithDatasets[T](f: StdPropertyGraph => Dataset[T])(graph: StdPropertyGraph) = {
    val plan = f(graph).queryExecution
    val result = f(graph)

    // warmup
    runAndTime(3)(f(graph))(_.count())

    val times = runAndTime()(f(graph))(_.count())

    DatasetsResult(times, plan, result, sparkSession)
  }

  def benchmarkCypherOnSpark(query: SupportedQuery)(graph: StdPropertyGraph) = {
    import graph.session.implicits._

    val result = graph.cypher(query).products.toDS

    val plan = result.queryExecution
    val count = result.count()
    val checksum = result.map(_.productElement(0).hashCode()).rdd.reduce(_ + _)

    // warmup
    println("Begin warmup")
    runAndTime(3)(graph.cypher(query))(_.products.count())

    println("Begin measurements")
    val times = runAndTime()(graph.cypher(query))(_.products.count())

    CypherOnSparkResult(times, plan, query, count, checksum)
  }

  abstract class ConfigOption[T](val name: String, val defaultValue: T)(convert: String => Option[T]) {
    def get(): T = Option(System.getProperty(name)).flatMap(convert).getOrElse(defaultValue)
  }

  object GRAPH_SIZE extends ConfigOption("cos.graph-size", 10000l)(x => Some(java.lang.Long.parseLong(x)))
  object MasterAddress extends ConfigOption("cos.master", "")(Some(_))
  object Logging extends ConfigOption("cos.logging", "OFF")(Some(_))
  object Parallelism extends ConfigOption("cos.parallelism", "8")(Some(_))
  object NodeFilePath extends ConfigOption("cos.nodeFile", "")(Some(_))
  object RelFilePath extends ConfigOption("cos.relFile", "")(Some(_))

  def createGraphFromNeo(size: Long) = {
    val (nodeRDD, relRDD) = Importers.importFromNeo(size)
    val nMapped = nodeRDD.map(internalNodeToCypherNode)
    val rMapped = relRDD.map(internalRelationshipToCypherRelationship)

    val parallelism = sparkSession.sparkContext.defaultParallelism

    val nodeSet = sparkSession.createDataset(nMapped)(CypherValue.Encoders.cypherNodeEncoder).repartition(parallelism).cache()
    val relSet = sparkSession.createDataset(rMapped)(CypherValue.Encoders.cypherRelationshipEncoder).repartition(parallelism).cache()

    new StdPropertyGraph(nodeSet, relSet)
  }

  def createExperimentalGraph(size: Long) = {
    val (nodeRDD, relRDD) = Importers.importFromNeo(size)
    val nMapped = nodeRDD.map(internalNodeToAccessControlNode)
    val rMapped = relRDD.map(internalRelToAccessControlRel)

//    val map = new mutable.HashMap[String, Seq[AccessControlRelationship]]
//    rels.collect().foreach {
//      case (t, rel) =>
//        val seq = map.getOrElse(t, Seq.empty[AccessControlRelationship])
//        map.put(t, seq :+ rel)
//    }
    val parallelism = sparkSession.sparkContext.defaultParallelism

    println("Repartitioning...")
    val nodeFrame = sparkSession.createDataFrame(nMapped).repartition(parallelism).cache()
    val relFrame = sparkSession.createDataFrame(rMapped).repartition(parallelism).cache()
//    val relFrames = map.map {
//      case (t, r) => t -> sparkSession.createDataFrame(r)
//    }
//    println(s"Done creating dataframes for $size relationships (${map.size} unique types)")

    ExperimentalGraph(nodeFrame, relFrame)
  }

  def benchmarkWithDataframes(f: (ExperimentalGraph => DataFrame))(graph: ExperimentalGraph) = {
    import graph.nodes.sparkSession.implicits._

    val result = f(graph)
    val count = result.count()
    val sum = result.map(row => row.apply(0).hashCode()).rdd.reduce(_ + _)

    // warmup
    runAndTime(3)(f(graph))(_.count)

    val times = runAndTime()(f(graph))(_.count)

    FrameResult(times, count, sum, result)
  }

  def main(args: Array[String]): Unit = {
    init()

    val nbr = GRAPH_SIZE.get()
    // create a CypherOnSpark graph
    val graph = createGraphFromNeo(nbr)
    val expGraph = createExperimentalGraph(nbr)
    println("Graph(s) created!")

//    val datasets = benchmarkWithDatasets(Dataset.nodeScanIdsSorted("Group", sparkSession))(graph)
//    val cypherOnSpark = benchmarkCypherOnSpark(NodeScanIdsSorted(IndexedSeq("Group")))(graph)
//    val rdds = benchmarkWithRdds(RDD.nodeScanIdsSorted("Group"))(graph)

//    val datasets = benchmarkWithDatasets(Dataset.simplePattern("Group", "ALLOWED_INHERIT", "Company", sparkSession))(graph)
//    val cypherOnSpark = benchmarkCypherOnSpark(SimplePattern(IndexedSeq("Group"), IndexedSeq("ALLOWED_INHERIT"), IndexedSeq("Company")))(graph)
//    val rdds = benchmarkWithRdds(RDDs.simplePattern("Group", "ALLOWED_INHERIT", "Company"))(graph)

    val query = SimplePatternIds(IndexedSeq("Group"), IndexedSeq("ALLOWED_INHERIT"), IndexedSeq("Company"))
//    val query = SimplePattern(IndexedSeq("Group"), IndexedSeq("ALLOWED_INHERIT"), IndexedSeq("Company"))

    val frames = benchmarkWithDataframes(DataFrames(query))(expGraph)
    val neo4j = benchmarkNeo4j(query.toString)
    val cypherOnSpark = benchmarkCypherOnSpark(query)(graph)

    println(s"We limited ourselves to ${if(nbr < 0) "ALL" else nbr} nodes and ${if(nbr < 0) "ALL" else nbr} relationships")
    println(s"Running query: $query")
    println(s"Using parallelism ${sparkSession.sparkContext.defaultParallelism}")

    compare(frames, neo4j, cypherOnSpark)
  }

  def compare[T](results: Result*) = {
    results.foreach { r =>
      println(s"$r: ${r.count} rows")
      println(s"$r: ${r.checksum} checksum")
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
  def count: Long
  def checksum: Int

  def avg = times.sum / times.length
}

case class CypherOnSparkResult(times: Seq[Long], _plan: QueryExecution, _query: SupportedQuery, count: Long, checksum: Int) extends Result {

  override def plan: String = _plan.toString()

  override def query: String = _query.toString

  override def toString: String = "CypherOnSpark"
}

case class DatasetsResult[T](times: Seq[Long], _plan: QueryExecution, _result: Dataset[T], sparkSession: SparkSession) extends Result {

  import sparkSession.implicits._

  override def plan: String = _plan.toString()

  override def query: String = "DATASET BENCHMARK"

  override def toString: String = "Datasets"

  override def count: Long = _result.count()

  override def checksum: Int = _result.map(_.hashCode()).rdd.reduce(_ + _)
}

case class RddResult(times: Seq[Long], count: Long, checksum: Int) extends Result {
  override def plan: String = "no plan for rdds"

  override def query: String = "RDD BENCHMARK"

  override def toString: String = "RDDs"
}

case class FrameResult(times: Seq[Long], count: Long, checksum: Int, _result: DataFrame) extends Result {
  override def plan: String = _result.queryExecution.toString()

  override def query: String = "DATAFRAME BENCHMARK W OTHER REPRESENTATION"

  override def toString: String = "DataFrames"
}

case class Neo4jResult(times: Seq[Long], count: Long, checksum: Int, plan: String, query: String) extends Result {
  override def toString: String = "Neo4j"
}

