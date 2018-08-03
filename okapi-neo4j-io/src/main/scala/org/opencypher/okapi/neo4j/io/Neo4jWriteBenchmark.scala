package org.opencypher.okapi.neo4j.io

import java.net.URI

import org.neo4j.driver.internal.value.ListValue
import org.neo4j.driver.v1.Values
import org.opencypher.okapi.impl.util.Measurement
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults.metaPropertyKey
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._

object Neo4jWriteBenchmark extends App {

  val config = Neo4jConfig(
    new URI("bolt://localhost"),
    "neo4j",
    Some("passwd")
  )

  def rowToListValue(data: Array[AnyRef]) = new ListValue(data.map(Values.value): _*)

  private val numberOfNodes = 10000
  val inputNodes = (1 to numberOfNodes).map { i =>
    Array[AnyRef](i.asInstanceOf[AnyRef], i.asInstanceOf[AnyRef], i.toString.asInstanceOf[AnyRef], (i % 2 == 0).asInstanceOf[AnyRef])
  }

  val inputRels = (2 to numberOfNodes + 1).map { i =>
    Array[AnyRef](i.asInstanceOf[AnyRef], (i - 1).asInstanceOf[AnyRef], i.asInstanceOf[AnyRef], (i % 2 == 0).asInstanceOf[AnyRef])
  }

  config.withSession { session =>
    session.run(s"CREATE CONSTRAINT ON (n:Foo) ASSERT n.$metaPropertyKey IS UNIQUE").consume()
  }

  val timings: Seq[Long] = (1 to 10).map { _ =>
    config.withSession { session =>
      session.run("MATCH (n) DETACH DELETE n").consume()
    }

    Measurement.time {
      EntityWriter.writeNodes(
        inputNodes.toIterator,
        Array(metaPropertyKey, "val1", "val2", "val3"),
        config,
        Set("Foo", "Bar", "Baz")
      )(rowToListValue)

      EntityWriter.writeRelationships(
        inputRels.toIterator,
        1,
        2,
        Array(metaPropertyKey, null, null, "val3"),
        config,
        "REL",
        Some("Foo")
      )(rowToListValue)
    }._2
  }

  println(s"MIN: ${timings.min}")
  println(s"MAX: ${timings.max}")
  println(s"AVG: ${timings.sum / timings.size}")
}
