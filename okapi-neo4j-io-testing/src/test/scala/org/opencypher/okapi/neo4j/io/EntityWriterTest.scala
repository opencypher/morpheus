package org.opencypher.okapi.neo4j.io

import org.neo4j.driver.internal.value.ListValue
import org.neo4j.driver.v1.Values
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults.metaPropertyKey
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.okapi.neo4j.io.testing.Neo4jServerFixture
import org.opencypher.okapi.testing.Bag._
import org.opencypher.okapi.testing.BaseTestSuite
import org.scalatest.BeforeAndAfter

import scala.collection.immutable

class EntityWriterTest extends BaseTestSuite with Neo4jServerFixture with BeforeAndAfter {

  it("can write nodes") {
    EntityWriter.writeNodes(
      inputNodes.toIterator,
      Array(metaPropertyKey, "val1", "val2", "val3", null),
      neo4jConfig,
      Set("Foo", "Bar", "Baz")
    )(rowToListValue)

    val expected = inputNodes.map { node =>
      CypherMap(
        s"n.$metaPropertyKey" -> node(0),
        "n.val1" -> node(1),
        "n.val2" -> node(2),
        "n.val3" -> node(3)
      )
    }.toBag

    val result = neo4jConfig.cypher(s"MATCH (n) RETURN n.$metaPropertyKey, n.val1, n.val2, n.val3").map(CypherMap).toBag
    result should equal(expected)
  }

  it("can write relationships") {
    EntityWriter.writeRelationships(
      inputRels.toIterator,
      1,
      2,
      Array(metaPropertyKey, null, null, "val3"),
      neo4jConfig,
      "REL",
      None
    )(rowToListValue)

    val expected = inputRels.map { rel =>
      CypherMap(
        s"r.$metaPropertyKey" -> rel(0),
        "r.val3" -> rel(3)
      )
    }.toBag

    val result = neo4jConfig.cypher(s"MATCH ()-[r]->() RETURN r.$metaPropertyKey, r.val3").map(CypherMap).toBag
    result should equal(expected)
  }

  override def dataFixture: String = ""

  private def rowToListValue(data: Array[AnyRef]) = new ListValue(data.map(Values.value): _*)

  private val numberOfNodes = 10
  val inputNodes: immutable.IndexedSeq[Array[AnyRef]] = (1 to numberOfNodes).map { i =>
    Array[AnyRef](
      i.asInstanceOf[AnyRef],
      i.asInstanceOf[AnyRef],
      i.toString.asInstanceOf[AnyRef],
      (i % 2 == 0).asInstanceOf[AnyRef],
      (i+1).asInstanceOf[AnyRef]
    )
  }

  val inputRels: immutable.IndexedSeq[Array[AnyRef]] = (2 to numberOfNodes).map { i =>
    Array[AnyRef](
      i.asInstanceOf[AnyRef],
      (i - 1).asInstanceOf[AnyRef],
      i.asInstanceOf[AnyRef],
      (i % 2 == 0).asInstanceOf[AnyRef]
    )
  }
}
