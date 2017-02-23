package org.opencypher.spark.prototype

import org.neo4j.cypher.internal.compiler.v3_2.AstRewritingMonitor
import org.neo4j.cypher.internal.frontend.v3_2.phases._
import org.neo4j.cypher.internal.frontend.v3_2.{CypherException, InputPosition}
import org.opencypher.spark.api.value.CypherString
import org.opencypher.spark.api.{CypherResultContainer, PropertyGraph}
import org.opencypher.spark.prototype.logical.LogicalOperatorProducer

import scala.reflect.ClassTag

trait Prototype {
  def cypher(query: String): CypherResultContainer = {
    val (stmt, params) = parser.parseAndExtract(query)

    val tokens = GlobalsExtractor(stmt)

    val ir = QueryDescriptorBuilder.from(stmt, query, tokens)

    val cvs = params.mapValues {
      case s: String => CypherString(s)
      case x => throw new UnsupportedOperationException(s"Can't convert $x to CypherType yet")
    }

    val plan = new LogicalOperatorProducer().plan(ir)

    val result = graph.cypherNew(ir, cvs)

    result
  }

  def graph: PropertyGraph

  val parser = CypherParser

}

case class FrontendContext() extends BaseContext {
  override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING

  override def notificationLogger: InternalNotificationLogger = devNullLogger

  override def exceptionCreator: (String, InputPosition) => CypherException = (_, _) => null

  override def monitors: Monitors = new Monitors {
    override def newMonitor[T <: AnyRef : ClassTag](tags: String*): T = {
      new AstRewritingMonitor {
        override def abortedRewriting(obj: AnyRef): Unit = ???
        override def abortedRewritingDueToLargeDNF(obj: AnyRef): Unit = ???
      }
    }.asInstanceOf[T]

    override def addMonitorListener[T](monitor: T, tags: String*) = ???
  }
}
