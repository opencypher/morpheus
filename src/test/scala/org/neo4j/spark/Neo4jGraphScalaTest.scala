package org.neo4j.spark

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit._
import org.neo4j.harness.{ServerControls, TestServerBuilders}

import scala.collection.JavaConverters._


/**
  * @author mh
  * @since 17.07.16
  */
class Neo4jGraphScalaTest {
  val FIXTURE: String = "CREATE (:A {a:0})-[:REL {foo:'bar'}]->(:B {b:1})"
  private var conf: SparkConf = null
  private var sc: JavaSparkContext = null
  private var server: ServerControls = null

  @Before
  @throws[Exception]
  def setUp {
    server = TestServerBuilders.newInProcessBuilder.withConfig("dbms.security.auth_enabled", "false").withFixture(FIXTURE).newServer
    conf = new SparkConf().setAppName("neoTest").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true").set("spark.neo4j.bolt.url", server.boltURI.toString)
    sc = SparkContext.getOrCreate(conf)
  }

  @After def tearDown {
    server.close
    sc.close
  }

  @Test def runCypherQueryWithParams {
    val data = List(Map("id"->1,"name"->"Test").asJava).asJava
    Executor.execute(sc.sc, "UNWIND {data} as row CREATE (n:Test {id:row.id}) SET n.name = row.name", Map(("data",data)))
  }
  @Test def runMatrixQuery {
    val graph = Neo4jGraph.loadGraph(sc.sc, "A", Seq.empty, "B")
    assertEquals(2, graph.vertices.count)
    assertEquals(1, graph.edges.count)
  }

  @Test def saveGraph {
    val edges : RDD[Edge[Long]] = sc.makeRDD(Seq(Edge(0,1,42L)))
    val graph = Graph.fromEdges(edges,-1)
    assertEquals(2, graph.vertices.count)
    assertEquals(1, graph.edges.count)
    Neo4jGraph.saveGraph(sc,graph,null,("REL","test"))
    assertEquals(42L,server.graph().execute("MATCH (:A)-[rel:REL]->(:B) RETURN rel.test as prop").columnAs("prop").next())
  }

  @Test def saveGraphMerge {
    val edges : RDD[Edge[Long]] = sc.makeRDD(Seq(Edge(0,1,42L)))
    val graph = Graph.fromEdges(edges,13L)
    assertEquals(2, graph.vertices.count)
    assertEquals(1, graph.edges.count)
    Neo4jGraph.saveGraph(sc,graph,"value",("FOOBAR","test"),Option("Foo","id"),Option("Bar","id"),merge = true)
    assertEquals(Map("fid"->0L,"bid"->1L,"rv"->42L,"fv"->13L,"bv"->13L).asJava,server.graph().execute("MATCH (foo:Foo)-[rel:FOOBAR]->(bar:Bar) RETURN {fid: foo.id, fv:foo.value, rv:rel.test,bid:bar.id,bv:bar.value} as data").columnAs("data").next())
  }
  @Test def saveGraphByNodeLabel {
    val edges : RDD[Edge[Long]] = sc.makeRDD(Seq(Edge(0,1,42L)))
    val graph = Graph.fromEdges(edges,-1)
    assertEquals(2, graph.vertices.count)
    assertEquals(1, graph.edges.count)
    Neo4jGraph.saveGraph(sc,graph,null,("REL","test"),Option(("A","a")),Option(("B","b")))
    assertEquals(42L,server.graph().execute("MATCH (:A)-[rel:REL]->(:B) RETURN rel.test as prop").columnAs("prop").next())
  }
  @Test def mergeGraphByNodeLabel {
    val edges : RDD[Edge[Long]] = sc.makeRDD(Seq(Edge(0,1,42L)))
    val graph = Graph.fromEdges(edges,-1)
    assertEquals(2, graph.vertices.count)
    assertEquals(1, graph.edges.count)
    Neo4jGraph.saveGraph(sc,graph,null,("REL2","test"),merge = true)
    assertEquals(42L,server.graph().execute("MATCH (:A)-[rel:REL2]->(:B) RETURN rel.test as prop").columnAs("prop").next())
  }

  @Test def saveGraphNodes {
    val nodes : RDD[(VertexId, Long)] = sc.makeRDD(Seq((0L,10L),(1L,20L)))
    val edges : RDD[Edge[Long]] = sc.makeRDD(Seq())
    val graph = Graph[Long,Long](nodes,edges,-1)
    assertEquals(2, graph.vertices.count)
    assertEquals(0, graph.edges.count)
    Neo4jGraph.saveGraph(sc,graph,"prop")
    assertEquals(10L,server.graph().execute("MATCH (a:A) WHERE id(a) = 0 RETURN a.prop as prop").columnAs("prop").next())
    assertEquals(20L,server.graph().execute("MATCH (b:B) WHERE id(b) = 1 RETURN b.prop as prop").columnAs("prop").next())
  }
}
