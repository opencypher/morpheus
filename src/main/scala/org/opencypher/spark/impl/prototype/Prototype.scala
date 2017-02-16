package org.opencypher.spark.impl.prototype

import org.neo4j.cypher.internal.compiler.v3_2.AstRewritingMonitor
import org.neo4j.cypher.internal.compiler.v3_2.phases.{CompilationPhases, CompilationState}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.phases._
import org.neo4j.cypher.internal.frontend.v3_2.{CypherException, InputPosition}
import org.opencypher.spark.api.{CypherResultContainer, PropertyGraph}

import scala.reflect.ClassTag

trait Prototype {
  def cypher(query: String): CypherResultContainer = {
    val (stmt, params) = parser.parseAndExtract(query)

    val tokens = TokenCollector(stmt)

    val ir = QueryReprBuilder.from(stmt, query, tokens, params.keySet)
    val plan = planner.plan(ir)

    val result = graph.cypher(plan)

    result
  }

  def graph: PropertyGraph

  val planner = new SupportedQueryPlanner

  val parser = CypherParser

}

//trait CypherResult[T] {
//  def get(): T
//}

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
