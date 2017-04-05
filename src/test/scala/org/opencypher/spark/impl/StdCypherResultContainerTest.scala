package org.opencypher.spark.impl

import java.io.{PrintWriter, StringWriter}

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{LongType, StringType}
import org.opencypher.spark.api.frame.EmbeddedRepresentation
import org.opencypher.spark.prototype.api.types.{CTInteger, CTString}
import org.opencypher.spark.{StdTestSuite, TestSession}

class StdCypherResultContainerTest extends StdTestSuite with TestSession.Fixture {

  test("should output a nice table") {
    val signature = new StdFrameSignature(Map(
      StdField('name -> CTString) -> StdSlot('a, CTString, 0, EmbeddedRepresentation(StringType)),
      StdField('age -> CTInteger) -> StdSlot('b, CTInteger, 1, EmbeddedRepresentation(LongType))
    ))
    val frame = new StdCypherFrame[Product](signature) {
      override protected def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
        val tuples: Seq[Product] = Seq("Mats" -> 29l, "Stefan" -> 36l, "Third guy" -> 55l, "Nobody" -> -1l)
        context.session.createDataset(tuples)(context.productEncoder(signature.slots))
      }
    }

    val resultContainer = new StdCypherResultContainer(null, frame)(new StdRuntimeContext(session, Map.empty))

    val out = new StringWriter()
    resultContainer.print(new PrintWriter(out))
    out.toString should equal(
      """|+---------------------------------------------+
         || name                 | age                  |
         |+---------------------------------------------+
         || 'Mats'               | 29 :: INTEGER        |
         || 'Stefan'             | 36 :: INTEGER        |
         || 'Third guy'          | 55 :: INTEGER        |
         || 'Nobody'             | -1 :: INTEGER        |
         |+---------------------------------------------+
         |""".stripMargin)
  }
}
