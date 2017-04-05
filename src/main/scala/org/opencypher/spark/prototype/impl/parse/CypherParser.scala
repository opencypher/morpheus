package org.opencypher.spark.prototype.impl.parse

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.phases._
import org.neo4j.cypher.internal.frontend.v3_2.{AstRewritingMonitor, CypherException, InputPosition}

import scala.reflect.ClassTag

object CypherParser extends CypherParser

trait CypherParser {

  def parse(query: String): Statement = {
    val (statement, _) = parseAndExtract(query)
    statement
  }

  def parseAndExtract(query: String): (Statement, Map[String, Any]) = {
    val startState = BaseStateImpl(query, None, null)
    val endState = pipeLine.transform(startState, ParserContext())
    val params = endState.extractedParams
    val rewritten = endState.statement
    rewritten -> params
  }

  private val pipeLine =
    CompilationPhases.parsing(RewriterStepSequencer.newPlain) andThen
    CompilationPhases.lateAstRewriting
}

case class ParserContext() extends BaseContext {
  override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING

  override def notificationLogger: InternalNotificationLogger = devNullLogger

  override def exceptionCreator: (String, InputPosition) => CypherException = (_, _) => null

  override def monitors: Monitors = new Monitors {
    override def newMonitor[T <: AnyRef : ClassTag](tags: String*): T = {
      new AstRewritingMonitor {
        override def abortedRewriting(obj: AnyRef): Unit = ()
        override def abortedRewritingDueToLargeDNF(obj: AnyRef): Unit = ()
      }
    }.asInstanceOf[T]

    override def addMonitorListener[T](monitor: T, tags: String*): Unit = ()
  }
}
