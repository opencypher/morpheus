package org.opencypher.spark.impl.parse

import org.neo4j.cypher.internal.frontend.v3_3.{InvalidSemanticsException, SemanticError, SemanticErrorDef, SyntaxException, UnsupportedOpenCypher}
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.Forced
import org.neo4j.cypher.internal.frontend.v3_3.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_3.phases._
import org.opencypher.spark.impl.CompilationStage

object CypherParser extends CypherParser {

  def defaultContext(query: String) = CypherForApacheSparkContext(query)

  case class CypherForApacheSparkContext(query: String) extends BlankBaseContext {
    override def errorHandler: (Seq[SemanticErrorDef]) => Unit = errors => errors.foreach {
      case e: SemanticError => throw new SyntaxException(e.msg, query, e.position)
      case _: UnsupportedOpenCypher => // do nothing
      case x => throw new IllegalArgumentException(s"Expected a semantic error, got $x")
    }
  }

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

