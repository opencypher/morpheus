package org.opencypher.spark.prototype.impl.convert

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.phases.{BaseStateImpl, CompilationPhases}
import org.opencypher.spark.prototype.FrontendContext

object CypherParser extends CypherParser

trait CypherParser {

  def parse(query: String) = {
    val (statement, _) = parseAndExtract(query)
    statement
  }

  def parseAndExtract(query: String): (Statement, Map[String, Any]) = {
    val startState = BaseStateImpl(query, None, null)
    val endState = pipeLine.transform(startState, FrontendContext())
    val params = endState.extractedParams
    val rewritten = endState.statement
    rewritten -> params
  }

  private val pipeLine =
    CompilationPhases.parsing(RewriterStepSequencer.newPlain) andThen
    CompilationPhases.lateAstRewriting
}
