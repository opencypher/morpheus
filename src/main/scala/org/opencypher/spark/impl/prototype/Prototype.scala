package org.opencypher.spark.impl.prototype

import org.neo4j.cypher.internal.compiler.v3_2.AstRewritingMonitor
import org.neo4j.cypher.internal.frontend.v3_2.phases._
import org.neo4j.cypher.internal.frontend.v3_2.{CypherException, InputPosition}
import org.opencypher.spark.api.value.CypherString
import org.opencypher.spark.api.{CypherResultContainer, PropertyGraph}

import scala.reflect.ClassTag

trait Prototype {
  def cypher(query: String): CypherResultContainer = {
    val (stmt, params) = parser.parseAndExtract(query)

    val tokens = TokenCollector(stmt)

    val ir = QueryReprBuilder.from(stmt, query, tokens, params.keySet)

    val cvs = params.mapValues {
      case s: String => CypherString(s)
      case x => throw new UnsupportedOperationException(s"Can't convert $x to CypherType yet")
    }

    val result = graph.cypherNew(ir, cvs)

    result
  }

  def graph: PropertyGraph

//  val planner = new SupportedQueryPlanner

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
