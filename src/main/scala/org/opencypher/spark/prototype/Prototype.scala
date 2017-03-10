package org.opencypher.spark.prototype

import org.neo4j.cypher.internal.frontend.v3_2.phases._
import org.neo4j.cypher.internal.frontend.v3_2.{AstRewritingMonitor, CypherException, InputPosition}
import org.opencypher.spark.api.{CypherResultContainer, PropertyGraph}
import org.opencypher.spark.prototype.api.value.CypherString
import org.opencypher.spark.prototype.impl.convert.{CypherParser, CypherQueryBuilder, GlobalsExtractor}
import org.opencypher.spark.prototype.impl.planner.LogicalPlanner

import scala.reflect.ClassTag

trait Prototype {
  def cypher(query: String): CypherResultContainer = {
    val (stmt, params) = parser.parseAndExtract(query)

    val globals = GlobalsExtractor(stmt)

    val ir = CypherQueryBuilder.from(stmt, query, globals)

    val cvs = params.mapValues {
      case s: String => CypherString(s)
      case x => throw new UnsupportedOperationException(s"Can't convert $x to CypherValue yet")
    }

    val plan = new LogicalPlanner().plan(ir)

    val result = graph.cypherNew(plan, globals, cvs)

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
