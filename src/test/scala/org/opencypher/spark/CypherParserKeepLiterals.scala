package org.opencypher.spark

import org.neo4j.cypher.internal.frontend.v3_3.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.{CNFNormalizer, Namespacer, Never, rewriteEqualityToInPredicate}
import org.neo4j.cypher.internal.frontend.v3_3.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_3.phases._
import org.neo4j.cypher.internal.frontend.v3_3.{SemanticErrorDef, SemanticState}
import org.opencypher.spark.impl.parse.{BlankBaseContext, CypherParser, sparkCypherRewriting}

object CypherParserKeepLiterals extends CypherParser {
  override val pipeLine =
    Parsing.adds(BaseContains[Statement]) andThen
      SyntaxDeprecationWarnings andThen
      PreparatoryRewriting andThen
      SemanticAnalysis(warn = true).adds(BaseContains[SemanticState]) andThen
                                                  // this is the change -- main pipeline always extracts literals
      AstRewriting(RewriterStepSequencer.newPlain, Never) andThen
      Namespacer andThen
      rewriteEqualityToInPredicate andThen
      CNFNormalizer andThen
      LateAstRewriting andThen
      sparkCypherRewriting

  case object ExpressionParsingContext extends BlankBaseContext {
    override def errorHandler: (Seq[SemanticErrorDef]) => Unit = _ => ()  // ignores all errors
  }
}
