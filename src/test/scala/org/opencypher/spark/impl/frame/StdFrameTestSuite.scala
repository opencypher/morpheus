package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark._
import org.opencypher.spark.api.{CypherFrameSignature, CypherType, Representation}
import org.opencypher.spark.impl.{PlanningContext, StdCypherFrame, StdRuntimeContext}
import org.opencypher.spark.impl.util.SlotSymbolGenerator

class StdFrameTestSuite extends StdTestSuite with TestSession.Fixture {

  implicit val planningContext = new PlanningContext(new SlotSymbolGenerator)
  implicit val runtimeContext = new StdRuntimeContext(session)

  implicit final class RichFrame[Out](val frame: StdCypherFrame[Out]) extends AnyRef {

    def frameResult(implicit context: StdRuntimeContext) = {
      val out = frame.run(context)
      out.columns should equal(frame.slots.map(_.sym.name))
      FrameResult(out)
    }

    final case class FrameResult(dataframe: Dataset[Out]) {
      def signature = frame.signature
      def toList = dataframe.collect().toList
      def toSet = dataframe.collect().toSet
    }
  }

  implicit final class RichFrameSignature(val sig: CypherFrameSignature) extends AnyRef {

    def shouldHaveFields(expected: (Symbol, CypherType)*): Unit = {
      sig.fields.map { field => field.sym -> field.cypherType } should equal(expected)
    }

    def shouldHaveFieldSlots(expected: (Symbol, Representation)*): Unit = {
      sig.fields.flatMap { f => sig.slot(f).map { s => f.sym -> s.representation } }.toSet should equal(expected.toSet)
    }
  }
}
