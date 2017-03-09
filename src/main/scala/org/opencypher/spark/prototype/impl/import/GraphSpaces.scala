package org.opencypher.spark.prototype.impl.`import`

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.neo4j.spark.Neo4j
import org.opencypher.spark.prototype.api.graph.{SparkCypherGraph, SparkGraphSpace}
import org.opencypher.spark.prototype.api.schema.{Schema, VerifiedSchema}

object GraphSpaces {
  def fromNeo4j(schema: VerifiedSchema)(implicit sc: SparkSession): SparkGraphSpace = {
    val neo4j = Neo4j(sc.sparkContext)

    val nodes = neo4j.cypher("CYPHER runtime=compiled MATCH (n) RETURN n").loadNodeRdds.map(row => row(0).asInstanceOf[InternalNode])
    val rels = neo4j.cypher("CYPHER runtime=compiled MATCH ()-[r]->() RETURN r").loadRowRdd.map(row => row(0).asInstanceOf[InternalRelationship])



    val baseGraph = new SparkCypherGraph {
      override def nodes = ???
      override def relationships = ???

      override def views = ???

      override def space = ???

      override def schema = ???
    }

    new SparkGraphSpace {
      override def base = baseGraph

      override def globals = ???
    }
  }

  private def toStructType(schema: Schema) = {

  }

  def configureNeo4jAccess(config: SparkConf)(url: String, user: String = "", pw: String = ""): SparkConf = {
    if (url.nonEmpty) config.set("spark.neo4j.bolt.url", url)
    if (user.nonEmpty) config.set("spark.neo4j.bolt.user", user)
    if (pw.nonEmpty) config.set("spark.neo4j.bolt.password", pw) else config
  }
}
