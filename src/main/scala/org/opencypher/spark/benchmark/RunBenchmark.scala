package org.opencypher.spark.benchmark

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}
import org.opencypher.spark.api.value._
import org.opencypher.spark.benchmark.Converters.{internalNodeToAccessControlNode, internalNodeToCypherNode, internalRelToAccessControlRel, internalRelationshipToCypherRelationship}
import org.opencypher.spark.impl._

import scala.collection.immutable.IndexedSeq

object RunBenchmark {

  implicit var sparkSession: SparkSession = _

  def init() = {
    val conf = new SparkConf(true)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "org.opencypher.spark.CypherKryoRegistrar")
    conf.set("spark.sql.crossJoin.enabled", "true")
    conf.set("spark.neo4j.bolt.password", Neo4jPassword.get())
    conf.set("spark.driver.memory", "471859200")
    // Enable to see if we cover enough
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.sql.shuffle.partitions", Partitions.get().toString)

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

  abstract class ConfigOption[T](val name: String, val defaultValue: T)(convert: String => Option[T]) {
    def get(): T = Option(System.getProperty(name)).flatMap(convert).getOrElse(defaultValue)
  }

  object GraphSize extends ConfigOption("cos.graph-size", -1l)(x => Some(java.lang.Long.parseLong(x)))
  object MasterAddress extends ConfigOption("cos.master", "")(Some(_))
  object Logging extends ConfigOption("cos.logging", "OFF")(Some(_))
  object Partitions extends ConfigOption("cos.shuffle-partitions", 40)(x => Some(java.lang.Integer.parseInt(x)))
  object Runs extends ConfigOption("cos.runs", 6)(x => Some(java.lang.Integer.parseInt(x)))
  object WarmUpRuns extends ConfigOption("cos.warmupRuns", 2)(x => Some(java.lang.Integer.parseInt(x)))
  object NodeFilePath extends ConfigOption("cos.nodeFile", "")(Some(_))
  object RelFilePath extends ConfigOption("cos.relFile", "")(Some(_))
  object Neo4jPassword extends ConfigOption("cos.neo4j-pw", "foo")(Some(_))
  object Benchmarks extends ConfigOption("cos.benchmarks", "fast")(Some(_))
  object Query extends ConfigOption("cos.query", 1)(x => Some(java.lang.Integer.parseInt(x)))

  def createStdPropertyGraphFromNeo(size: Long) = {
    val session = sparkSession
    import CypherValue.Encoders._

    val (nodeRDD, relRDD) = Importers.importFromNeo(size)
    val nMapped = nodeRDD.map(internalNodeToCypherNode)
    val rMapped = relRDD.map(internalRelationshipToCypherRelationship)

    val nodeSet = session.createDataset[CypherNode](nMapped)
    val cachedNodeSet = nodeSet.repartition().cache()
    val relSet = session.createDataset[CypherRelationship](rMapped)
    val cachedRelSet = relSet.repartition().cache()

    new StdPropertyGraph(cachedNodeSet, cachedRelSet)
  }

  def createSimpleDataFrameGraph(size: Long) = {
    val (nodeRDD, relRDD) = Importers.importFromNeo(size)
    val nMapped: RDD[AccessControlNode] = nodeRDD.map(internalNodeToAccessControlNode)
    val rMapped = relRDD.map(internalRelToAccessControlRel)

    println("Repartitioning...")
    val nodeFrame = sparkSession.createDataFrame(nMapped)
    val idCol = nodeFrame.col("id")
    val cachedNodeFrame = cacheNow( nodeFrame.repartition(idCol).sortWithinPartitions(idCol) )
    val labeledNodes = Map(
      "Account" -> cacheNow( cachedNodeFrame.filter(cachedNodeFrame.col("account")) ),
      "Administrator" -> cacheNow( cachedNodeFrame.filter(cachedNodeFrame.col("administrator")) ),
      "Company" -> cacheNow( cachedNodeFrame.filter(cachedNodeFrame.col("company")) ),
      "Employee" -> cacheNow( cachedNodeFrame.filter(cachedNodeFrame.col("employee")) ),
      "Group" -> cacheNow( cachedNodeFrame.filter(cachedNodeFrame.col("group")) ),
      "Resource" -> cacheNow( cachedNodeFrame.filter(cachedNodeFrame.col("resource")) )
    )

    cachedNodeFrame.unpersist()

    val relFrame = sparkSession.createDataFrame(rMapped)
    val startIdCol = relFrame.col("startId")
    val cachedStartRelFrame = relFrame.repartition(startIdCol).sortWithinPartitions(startIdCol).cache()
    val endIdCol = relFrame.col("endId")
    val cachedEndRelFrame = relFrame.repartition(endIdCol).sortWithinPartitions(endIdCol).cache()
    val typCol = relFrame.col("typ")
    val types = cachedStartRelFrame.select(typCol).distinct().rdd.map(_.getString(0)).collect()
    val rels = types.map { typ =>
      typ ->
        (cacheNow( cachedStartRelFrame.filter(typCol === typ) ) -> cacheNow( cachedEndRelFrame.filter(typCol === typ)))
    }.toMap

    cachedStartRelFrame.unpersist()
    cachedEndRelFrame.unpersist()

    SimpleDataFrameGraph(labeledNodes, rels)
  }

  private def cacheNow[T](dataset: Dataset[T]): Dataset[T] = {
    val result = dataset.cache()
    result.rdd.localCheckpoint()
    result
  }

  def createNeo4jViaDriverGraph: Neo4jViaDriverGraph = {
    new Neo4jViaDriverGraph(
      GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", Neo4jPassword.get()))
    )
  }

  val queries = Map(
    1 -> SimplePatternIds(IndexedSeq("Employee"), IndexedSeq("HAS_ACCOUNT"), IndexedSeq("Account")),
    2 -> SimplePatternIds(IndexedSeq("Group"), IndexedSeq("ALLOWED_INHERIT"), IndexedSeq("Company")),
    3 -> MidPattern("Employee", "WORKS_FOR", "Company", "CHILD_OF", "Company"),
    4 -> FixedLengthPattern("Administrator", Seq(Out("MEMBER_OF") -> "Group", Out("ALLOWED_DO_NOT_INHERIT") -> "Company", Out("CHILD_OF") -> "Company")),
    5 -> FixedLengthPattern("Resource", Seq(Out("WORKS_FOR") -> "Company", In("ALLOWED_DO_NOT_INHERIT") -> "Group", Out("ALLOWED_INHERIT") -> "Company"))
  )

  def loadQuery(): SupportedQuery = {
    queries.get(Query.get()) match {
      case Some(q) => q
      case None =>
        println(s"Unknown query specified: ${Query.get()}")
        println(s"Known queries are: $queries")
        throw new IllegalArgumentException(s"No such query ${Query.get()}")
    }
  }

  def main(args: Array[String]): Unit = {
    init()

    val graphSize = GraphSize.get()

    lazy val stdGraph = createStdPropertyGraphFromNeo(graphSize)
    lazy val sdfGraph = createSimpleDataFrameGraph(graphSize)
    lazy val neoGraph = createNeo4jViaDriverGraph
    println("Graph(s) created!")

    val query = loadQuery()

    lazy val frameResult = DataFrameBenchmarks(query).using(sdfGraph)
    lazy val rddResult = RDDBenchmarks(query).using(stdGraph)
    lazy val neoResult = Neo4jViaDriverBenchmarks(query).using(neoGraph)
    lazy val cosResult = CypherOnSparkBenchmarks(query).using(stdGraph)
    lazy val benchmarksAndGraphs = Benchmarks.get() match {
      case "all" =>
        Seq(
          neoResult
          , frameResult
          , rddResult
          , cosResult
        )
      case "fast" =>
        Seq(frameResult, neoResult)
    }

    neoResult.use { (benchmark, graph) =>
      println(BenchmarkSummary(query.toString, benchmark.numNodes(graph), benchmark.numRelationships(graph)))
    }
    println(s"GraphSize used by spark benchmarks is $graphSize.")
    println(s"Using ${Partitions.get()} partitions.")

    val results = benchmarksAndGraphs.map { benchmarkAndGraph =>
      benchmarkAndGraph.use { (benchmark, graph) =>
        println(s"About to benchmark ${benchmark.name.trim}")
        BenchmarkSeries.run(benchmarkAndGraph)
      }
    }
    results.foreach(println)
  }
}
