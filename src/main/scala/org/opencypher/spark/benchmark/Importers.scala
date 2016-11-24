package org.opencypher.spark.benchmark

import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.neo4j.spark.Neo4j

object Importers {
  def importFromNeo(size: Long) = {
    val neo4j = Neo4j(Benchmark.sparkSession.sparkContext)

    if (size > 0) limit(neo4j, size)
    else all(neo4j)
  }

  private def limit(neo4j: Neo4j, limit: Long) = {
    println(s"Importing $limit nodes and relationships from Neo4j")
    val nodes = neo4j.cypher(s"MATCH (b)-->(a) WITH a, b LIMIT $limit UNWIND [a, b] AS n RETURN DISTINCT n").loadNodeRdds.map(row => row(0).asInstanceOf[InternalNode])

    val rels = neo4j.cypher(s"MATCH ()-[r]->() RETURN r LIMIT $limit").loadRowRdd.map(row => row(0).asInstanceOf[InternalRelationship])

    nodes -> rels
  }

  private def all(neo4j: Neo4j) = {
    println(s"Importing ALL nodes and relationships from Neo4j")
    val nodes = neo4j.cypher("CYPHER runtime=compiled MATCH (n) RETURN n").loadNodeRdds.map(row => row(0).asInstanceOf[InternalNode])

    val rels = neo4j.cypher("CYPHER runtime=compiled MATCH ()-[r]->() RETURN r").loadRowRdd.map(row => row(0).asInstanceOf[InternalRelationship])

    nodes -> rels
  }

}
