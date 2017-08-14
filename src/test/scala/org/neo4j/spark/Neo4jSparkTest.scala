package org.neo4j.spark

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit._
import org.neo4j.harness.{ServerControls, TestServerBuilders}


/**
  * @author mh
  * @since 17.07.16
  */
class Neo4jSparkTest {
  val FIXTURE: String =
      """
      UNWIND range(1,100) as id
      CREATE (p:Person {id:id}) WITH collect(p) as people
      UNWIND people as p1
      UNWIND range(1,10) as friend
      WITH p1, people[(p1.id + friend) % size(people)] as p2
      CREATE (p1)-[:KNOWS]->(p2)
      """
  private var conf: SparkConf = _
  private var sc: SparkContext = _
  private var server: ServerControls = _

  @Before
  @throws[Exception]
  def setUp() {
    server = TestServerBuilders.newInProcessBuilder.withConfig("dbms.security.auth_enabled", "false").withFixture(FIXTURE).newServer
    conf = new SparkConf().setAppName("neoTest").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true").set("spark.neo4j.bolt.url", server.boltURI.toString)
    sc = SparkContext.getOrCreate(conf)
  }

  @After def tearDown() {
    server.close()
  }

  @Test def runCypherQueryWithParams() {
    val neo4j: Neo4j = Neo4j(sc).cypher("MATCH (n:Person) WHERE n.id <= {maxId} RETURN id(n)").param("maxId", 10)
    assertEquals(10, neo4j.loadRowRdd.count())
  }
  @Test def runCypherQuery() {
    val neo4j: Neo4j = Neo4j(sc).cypher("MATCH (n:Person) RETURN id(n)")
    val people: Long = neo4j.loadRowRdd.count()
    assertEquals(100,people)
  }

  @Test(expected = classOf[RuntimeException]) def runCypherQueryNoResults() {
    val neo4j: Neo4j = Neo4j(sc).cypher("MATCH (n:Person) WHERE false RETURN id(n)")
    val people: Long = neo4j.loadDataFrame.count()
    assertEquals(0,people)
  }
  @Test def runCypherQueryNoResultsWithSchema() {
    val neo4j: Neo4j = Neo4j(sc).cypher("MATCH (n:Person) WHERE false RETURN id(n) as id")
    val people: Long = neo4j.loadDataFrame("id" -> "long").count()
    assertEquals(0,people)
  }

  @Test def runCypherQueryWithPartition() {
    val neo4j: Neo4j = Neo4j(sc).cypher("MATCH (n:Person) RETURN id(n) SKIP {_skip} LIMIT {_limit}").partitions(4).batch(25)
    val people: Long = neo4j.loadRowRdd.count()
    assertEquals(100,people)
  }
  @Test def runCypherQueryDataFrameWithPartition() {
    val neo4j: Neo4j = Neo4j(sc).cypher("MATCH (n:Person) RETURN id(n) as id SKIP {_skip} LIMIT {_limit}").partitions(4).batch(25)
    val df: DataFrame = neo4j.loadDataFrame
    assertEquals(1, df.schema.fieldNames.length)
    assertEquals("id", df.schema.fieldNames(0))
    assertEquals("long", df.schema.apply("id").dataType.typeName)
    val people: Long = df.count()
    assertEquals(100,people)
  }

  @Test def runPatternQueryWithPartition() {
    val neo4j: Neo4j = Neo4j(sc).pattern("Person",Seq("KNOWS"),"Person").rows(80).batch(21)
    val people: Long = neo4j.loadRowRdd.count()
    assertEquals(80,people)
  }
  @Test def runPatternRelQueryWithPartition() {
    val neo4j: Neo4j = Neo4j(sc).pattern("Person",Seq("KNOWS"),"Person").partitions(12).batch(100)
    val knows: Long = neo4j.loadRelRdd.count()
    assertEquals(1000,knows)
  }
  @Test def runCypherRelQueryWithPartition() {
    val neo4j: Neo4j = Neo4j(sc).cypher("MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN id(n) as src,id(m) as dst,type(r) as value SKIP {_skip} LIMIT {_limit}").partitions(7).batch(200)
    val knows: Long = neo4j.loadRowRdd.count()
    assertEquals(1000,knows)
  }
  @Test def runCypherRelQueryWithPartitionGraph() {
    val neo4j: Neo4j = Neo4j(sc).rels("MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN id(n) as src, id(m) as dst, type(r) as value SKIP {_skip} LIMIT {_limit}").partitions(7).batch(200)
    val graph: Graph[Long, String] = neo4j.loadGraph[Long,String]
    assertEquals(100,graph.vertices.count())
    assertEquals(1000,graph.edges.count())
  }
  @Test def runPatternRelQueryWithPartitionGraph() {
    val neo4j: Neo4j = Neo4j(sc).pattern(("Person","id"),("KNOWS",null),("Person","id")).partitions(7).batch(200)
    val graph: Graph[_, String] = neo4j.loadGraph[Long,String]
    assertEquals(100,graph.vertices.count())
    assertEquals(1000,graph.edges.count())
  }

  @Test def runSimplePatternRelQueryWithPartitionGraph() {
    val neo4j: Neo4j = Neo4j(sc).pattern("Person",Seq("KNOWS"), "Person").partitions(7).batch(200)
    val graph: Graph[_, _] = neo4j.loadGraph[Unit,Unit]
    assertEquals(100,graph.vertices.count())
    assertEquals(1000,graph.edges.count())

    val top3: Array[(VertexId, Double)] = PageRank.run(graph,5).vertices.sortBy(v => v._2, ascending = false,5).take(3)
    assertEquals(0.622D, top3(0)._2, 0.01)
  }

  //  @Test def runSimplePatternRelQueryWithPartitionGraphFrame() {
//    val neo4j: Neo4j = Neo4j(sc).pattern(("Person","id"),("KNOWS",null), ("Person","id")).partitions(7).batch(200)
//    val graphFrame: GraphFrame = neo4j.loadGraphFrame
//    assertEquals(100,graphFrame.vertices.count)
//    assertEquals(1000,graphFrame.edges.count)
//
//    val pageRankFrame: GraphFrame = graphFrame.pageRank.maxIter(5).run()
//    val ranked: DataFrame = pageRankFrame.vertices
//    ranked.printSchema()
//    // sorting DF http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column
//    val top3: Array[Row] = ranked.orderBy(ranked.col("pagerank").desc).take(3)
//    top3.foreach(println)
//    assertEquals(0.622D, top3(0).getAs[Double]("pagerank"), 0.01)
//  }

  /*
  @Test def runMatrixQuery() {
    val neo4j: Neo4j = Neo4j(sc).pattern("Person", Seq.empty, "Person")
    val graph = neo4j.loadGraph
    assertEquals(100, graph.vertices.count)
    assertEquals(1000, graph.edges.count)
  }
*/
}
