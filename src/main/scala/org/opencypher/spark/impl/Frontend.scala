package org.opencypher.spark.impl

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.parser.CypherParser
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types.{CTMap, CTNode, CTWildcard}

object Frontend {

  val parser = new CypherParser

  def parse(query: String): Statement =
    parser.parse(query)
}

object TypeContext {

  def init(statement: Statement, schema: Schema): TypeContext = {
    TypeContext(statement, schema).buildPatternTypeTable()
  }

}

case class TypeContext(statement: Statement, schema: Schema,
                       patternTypeTable: Map[Variable, CypherType] = Map.empty,
                       exprTypeTable: Map[Expression, CypherType] = Map.empty) {

  private def buildPatternTypeTable(): TypeContext = {
    val things = statement match {
      case Query(_, SingleQuery(clauses)) =>
        clauses.map(inferPatterns)
      case _ => ???
    }

    copy(patternTypeTable = things.flatten.toMap)
  }

  def inferExpressions(): TypeContext = {
    val things = statement match {
      case Query(_, SingleQuery(clauses)) =>
        clauses.map(inferExpressions)
      case _ => ???
    }

    copy(exprTypeTable = things.flatten.toMap)
  }

  private def inferPatterns(clause: Clause): Seq[(Variable, CypherType)] = {
    clause match {
      case Match(false, pattern, _, _) =>
        pattern.patternParts.map { part =>
          part.element match {
            case NodePattern(None, _, _) => ???
            case NodePattern(Some(variable), labels, properties) => variable -> CTNode(labels.map(_.name).toSet)
            case _ => ???
          }
        }
      case Return(_, _, _, _, _, _) => Seq.empty
    }
  }

  private def inferExpressions(clause: Clause): Seq[(Expression, CypherType)] = {
    clause match {
      case Match(false, _, _, _) => Seq.empty
      case Return(_, ReturnItems(_, items), _, _, _, _) => items.map {
        case AliasedReturnItem(_, _) => ???
        case UnaliasedReturnItem(expression, _) => expression -> inferType(expression)
      }
    }
  }

  private def inferType(expr: Expression): CypherType = {
    expr match {
      case Property(v: Variable, PropertyKeyName(name)) =>
        patternTypeTable.getOrElse(v, CTMap) match {
          case CTNode(labels) =>
            val keys = labels.map(schema.nodeKeys)
            val combinedKeys = keys.reduce(_ ++ _)
            combinedKeys(name)
          case _ => ???
        }
      case _ => CTWildcard
    }
  }

//  - Variable -> CypherType
//  - Expr -> CypherType
//  - Function Types
//  - Schema
//  - Utility functions for working with the schema
}
