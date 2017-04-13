package org.opencypher.spark.impl.parse

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters.Forced
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.phases._
import org.opencypher.spark.impl.CompilationStage

object CypherParser extends CypherParser {
  implicit object defaultContext extends BlankBaseContext
}

trait CypherParser extends CompilationStage[String, Statement, BaseContext] {

  override type Out = (Statement, Map[String, Any])

  override def extract(output: (Statement, Map[String, Any])) = output._1

  override def process(query: String)(implicit context: BaseContext): (Statement, Map[String, Any]) = {
    val startState = BaseStateImpl(query, None, null)
    val endState = pipeLine.transform(startState, context)
    val params = endState.extractedParams
    val rewritten = endState.statement
    rewritten -> params
  }

  protected val pipeLine =
    CompilationPhases.parsing(RewriterStepSequencer.newPlain, Forced) andThen
    CompilationPhases.lateAstRewriting andThen sparkCypherRewriting

}

