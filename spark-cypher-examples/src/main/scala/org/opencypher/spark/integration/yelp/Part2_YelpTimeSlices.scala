package org.opencypher.spark.integration.yelp

import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpConstants.{defaultYelpGraphFolder, yelpFullGraphName, _}

object Part2_YelpTimeSlices extends App {

  lazy val inputPath = args.headOption.getOrElse(defaultYelpGraphFolder)

  implicit val caps: CAPSSession = CAPSSession.local()

  neo4jConfig.withSession { session =>
    session.run("MATCH (n) DETACH DELETE n").consume()
    session.run("DROP CONSTRAINT ON ( ___pre2017:___pre2017 ) ASSERT ___pre2017.___capsID IS UNIQUE").consume()
    session.run("DROP CONSTRAINT ON ( ___since2017:___since2017 ) ASSERT ___since2017.___capsID IS UNIQUE").consume()
  }

  import caps._

  registerSource(fsNamespace, GraphSources.fs(inputPath).parquet)
  registerSource(neo4jNamespace, GraphSources.cypher.neo4j(neo4jConfig))

  // Construct Phoenix sub graph
  cypher(
    s"""
       |CATALOG CREATE GRAPH $phoenixGraphName {
       |  FROM GRAPH $fsNamespace.$yelpFullGraphName
       |  MATCH (b:Business)<-[r:REVIEWS]-(user:User)
       |  WHERE b.city = 'Phoenix'
       |  CONSTRUCT
       |   CREATE (user)-[r]->(b)
       |  RETURN GRAPH
       |}
      """.stripMargin)

  // Construct reviews before 2017
  cypher(
    s"""
       |CATALOG CREATE GRAPH $neo4jNamespace.$pre2017GraphName {
       |  FROM GRAPH $phoenixGraphName
       |  MATCH (b:Business)<-[r:REVIEWS]-(user:User)
       |  WHERE r.date.year < 2017
       |  CONSTRUCT
       |   CREATE (user)-[r]->(b)
       |  RETURN GRAPH
       |}
     """.stripMargin)

  // Construct reviews since 2017
  cypher(
    s"""
       |CATALOG CREATE GRAPH $neo4jNamespace.$since2017GraphName {
       |  FROM GRAPH $phoenixGraphName
       |  MATCH (b:Business)<-[r:REVIEWS]-(user:User)
       |  WHERE r.date.year >= 2017
       |  CONSTRUCT
       |   CREATE (user)-[r]->(b)
       |  RETURN GRAPH
       |}
     """.stripMargin)
}
