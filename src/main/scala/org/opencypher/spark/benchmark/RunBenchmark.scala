package org.opencypher.spark.benchmark

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.graphframes.GraphFrame
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}
import org.opencypher.spark.prototype.api.value._
import org.opencypher.spark.benchmark.Configuration._
import org.opencypher.spark.benchmark.Converters.{internalNodeToAccessControlNode, internalNodeToCypherNode, internalRelToAccessControlRel, internalRelationshipToCypherRelationship}
import org.opencypher.spark.impl._

import scala.collection.immutable.IndexedSeq

object RunBenchmark {

  implicit lazy val sparkSession: SparkSession = {
    val conf = new SparkConf(true)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "org.opencypher.spark.CypherKryoRegistrar")
//    conf.set("spark.sql.crossJoin.enabled", "false")
    conf.set("spark.neo4j.bolt.password", Neo4jPassword.get())
    // Enable to see if we cover enough
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.sql.shuffle.partitions", Partitions.get().toString)

    //
    // This may or may not help - depending on the query
    // conf.set("spark.kryo.referenceTracking","false")
    //

    val session = SparkSession.builder()
      .config(conf)
      .master(MasterAddress.get())
      .appName(s"cypher-on-spark-benchmark-${Calendar.getInstance().getTime}")
      .getOrCreate()
    session.sparkContext.setLogLevel(Logging.get())
    session
  }

  def loadRDDs(): (RDD[CypherNode], RDD[CypherRelationship]) = {
    val nodeRDD = sparkSession.sparkContext.objectFile[CypherNode](NodeFilePath.get())
    val relsRDD = sparkSession.sparkContext.objectFile[CypherRelationship](RelFilePath.get())

    Tests.printSums(nodeRDD, relsRDD)

    nodeRDD -> relsRDD
  }

  private def createGraph(size: Long) = {
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

    println("Graph cached!")

    new StdPropertyGraph(cachedNodeSet, cachedRelSet)
  }

  def createGraphFrameGraph(size: Long): GraphFrame = {
    val (nodeRDD, relRDD) = importAndConvert(size)

    printf("Creating GraphFrame graph ... ")
    val nodeFrame = sparkSession.createDataFrame(nodeRDD)
    val idCol = nodeFrame.col("id")
    val cachedNodeFrame = cacheNow(nodeFrame.repartition(idCol).sortWithinPartitions(idCol))

    val relFrame = sparkSession.createDataFrame(relRDD).toDF("id", "src", "dst", "type")
    val srcCol = relFrame.col("src")
    val cachedStartRelFrame = cacheNow(relFrame.repartition(srcCol).sortWithinPartitions(srcCol))
    println("Finished GraphFrame graph!")

    GraphFrame(cachedNodeFrame, cachedStartRelFrame)
  }

  // TODO: Solve this less ugly
  private var cNodeRDD: RDD[AccessControlNode] = _
  private var cRelRDD: RDD[AccessControlRelationship] = _

  private def importAndConvert(size: Long): (RDD[AccessControlNode], RDD[AccessControlRelationship]) = {
    if (cNodeRDD == null || cRelRDD == null) {
      val (nodeRDD, relRDD) = Importers.importFromNeo(size)
      cNodeRDD = nodeRDD.map(internalNodeToAccessControlNode)
      cRelRDD = relRDD.map(internalRelToAccessControlRel)
    }
    cNodeRDD -> cRelRDD
  }

  // TODO: Solve this less ugly
  private var cLabeledNodes: Map[String, Dataset[Row]] = _
  private var cNodeFrame: DataFrame = _
  private var cIdCol: Column = _

  private def cacheNodeMap(nodeRDD: RDD[AccessControlNode]) = {
    if (cLabeledNodes == null || cNodeFrame == null || cIdCol == null) {
      printf("Creating node frame map... ")
      cNodeFrame = sparkSession.createDataFrame(nodeRDD)
      cIdCol = cNodeFrame.col("id")
      cLabeledNodes = Map(
        "Account" -> cacheNow(cNodeFrame.filter(cNodeFrame.col("account")).repartition(cIdCol).sortWithinPartitions(cIdCol)),
        "Administrator" -> cacheNow(cNodeFrame.filter(cNodeFrame.col("administrator")).repartition(cIdCol).sortWithinPartitions(cIdCol)),
        "Company" -> cacheNow(cNodeFrame.filter(cNodeFrame.col("company")).repartition(cIdCol).sortWithinPartitions(cIdCol)),
        "Employee" -> cacheNow(cNodeFrame.filter(cNodeFrame.col("employee")).repartition(cIdCol).sortWithinPartitions(cIdCol)),
        "Group" -> cacheNow(cNodeFrame.filter(cNodeFrame.col("group")).repartition(cIdCol).sortWithinPartitions(cIdCol)),
        "Resource" -> cacheNow(cNodeFrame.filter(cNodeFrame.col("resource")).repartition(cIdCol).sortWithinPartitions(cIdCol))
      )
      println(" Done!")
    }
    (cLabeledNodes, cNodeFrame, cIdCol)
  }

  def createTripletGraph(size: Long): TripletGraph = {
    val (nodeRDD, relRDD) = importAndConvert(size)

    val (labeledNodes, nodeFrame, idCol) = cacheNodeMap(nodeRDD)

    printf("Creating triplet views ... ")
    val relFrame = sparkSession.createDataFrame(relRDD)
    val startIdCol = relFrame.col("startId")
    val endIdCol = relFrame.col("endId")

    // join 'other' (source/target) node labels into triplet view
    val outTriplet = relFrame.join(nodeFrame, endIdCol === idCol).drop(idCol)
    val inTriplet = relFrame.join(nodeFrame, startIdCol === idCol).drop(idCol)

    val typCol = relFrame.col("typ")
    val types = relFrame.select(typCol).distinct().rdd.map(_.getString(0)).collect()
    val rels = types.map { typ =>
      typ ->
        (cacheNow(outTriplet.filter(typCol === typ).repartition(startIdCol).sortWithinPartitions(startIdCol))
          -> cacheNow(inTriplet.filter(typCol === typ).repartition(endIdCol).sortWithinPartitions(endIdCol)))
    }.toMap
    println("Finished triplet views!")

    TripletGraph(labeledNodes, rels)
  }

  def createSimpleDataFrameGraph(size: Long): SimpleDataFrameGraph = {
    val (nodeRDD, relRDD) = importAndConvert(size)

    val (labeledNodes, _, _) = cacheNodeMap(nodeRDD)

    printf("Creating relationship views ... ")
    val relFrame = sparkSession.createDataFrame(relRDD)
    val startIdCol = relFrame.col("startId")
    val endIdCol = relFrame.col("endId")
    val typCol = relFrame.col("typ")
    val types = relFrame.select(typCol).distinct().rdd.map(_.getString(0)).collect()
    val rels = types.map { typ =>
      typ ->
        (cacheNow(relFrame.filter(typCol === typ).repartition(startIdCol).sortWithinPartitions(startIdCol)) ->
          cacheNow(relFrame.filter(typCol === typ).repartition(endIdCol).sortWithinPartitions(endIdCol)))
    }.toMap
    println("Finished relationship views!")

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
    5 -> FixedLengthPattern("Resource", Seq(Out("WORKS_FOR") -> "Company", In("ALLOWED_DO_NOT_INHERIT") -> "Group", Out("ALLOWED_INHERIT") -> "Company")),
    // almost the same as #3
    6 -> FixedLengthPattern("Employee", Seq(Out("WORKS_FOR") -> "Company", Out("CHILD_OF") -> "Company")),
    // same as #1
    7 -> FixedLengthPattern("Employee", Seq(Out("HAS_ACCOUNT") -> "Account")),
    // same as #2
    8 -> FixedLengthPattern("Group", Seq(Out("ALLOWED_INHERIT") -> "Company")),
    // match (:Entry)-[:MEMBER_OF_BAND]->(:Entry)-[:FROM_AREA]->(:Area)-[:PART_OF]-(:Subdivision) return
    10 -> FixedLengthPattern("Entry", Seq(Out("MEMBER_OF_BAND") -> "Entry", Out("FROM_AREA") -> "Area", Out("PART_OF") -> "Subdivision"))
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

  def parseArgs(args: Array[String]): Unit = {
    args.foreach { s =>
      val split = s.split("=")
      System.setProperty(split(0), split(1))
    }
  }

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    // print conf
    Configuration.print()

    val graphSize = GraphSize.get()

    lazy val stdGraph = createStdPropertyGraphFromNeo(graphSize)
    lazy val sdfGraph = createSimpleDataFrameGraph(graphSize)
    lazy val neoGraph = createNeo4jViaDriverGraph
    lazy val graphFrame = createGraphFrameGraph(graphSize)
    lazy val tripletGraph = createTripletGraph(graphSize)

    val query = loadQuery()

    lazy val frameResult = DataFrameBenchmarks(query).using(sdfGraph)
    lazy val rddResult = RDDBenchmarks(query).using(stdGraph)
    lazy val neoResult = Neo4jViaDriverBenchmarks(query).using(neoGraph)
    lazy val cosResult = CypherOnSparkBenchmarks(query).using(stdGraph)
    lazy val graphFrameResult = GraphFramesBenchmarks(query).using(graphFrame)
    lazy val tripletResult = TripletBenchmarks(query).using(tripletGraph)

    lazy val benchmarksAndGraphs = Benchmarks.get() match {
      case "all" =>
        Seq(
          neoResult
          , frameResult
          , rddResult
          , cosResult
        )
      case "fast" =>
        Seq(
          tripletResult,
          frameResult,
          graphFrameResult,
          neoResult
        )
      case "frames" =>
        Seq(
          tripletResult,
          frameResult,
          graphFrameResult
        )
    }

    neoResult.use { (benchmark, graph) =>
      println(BenchmarkSummary(query.toString, benchmark.numNodes(graph), benchmark.numRelationships(graph)))
    }

    val results = benchmarksAndGraphs.map { benchmarkAndGraph =>
      benchmarkAndGraph.use { (benchmark, _) =>
        println(s"About to benchmark ${benchmark.name.trim}")
        BenchmarkSeries.run(benchmarkAndGraph)
      }
    }.sortBy(_.avg)
    results.foreach(r => println(r.summary(Some(results.last.avg))))
  }
}
