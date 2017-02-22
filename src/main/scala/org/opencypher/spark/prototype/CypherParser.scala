package org.opencypher.spark.prototype

import org.neo4j.cypher.internal.compiler.v3_2.phases.{CompilationPhases, CompilationState}
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStepSequencer

object CypherParser extends CypherParser

trait CypherParser {

  def parse(query: String) = {
    val (statement, _) = parseAndExtract(query)
    statement
  }

  def parseAndExtract(query: String): (Statement, Map[String, Any]) = {
    val startState = CompilationState(query, None, null)
    val endState = pipeLine.transform(startState, FrontendContext())
    val params = endState.extractedParams
    val rewritten = endState.statement
    rewritten -> params
  }

  private val pipeLine =
    CompilationPhases.parsing(RewriterStepSequencer.newPlain) andThen
    CompilationPhases.lateAstRewriting
}
