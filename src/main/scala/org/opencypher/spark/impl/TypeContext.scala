package org.opencypher.spark.impl

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types.{CTNode, CTRelationship}

sealed trait TypingError
case class MissingVariable(v: Variable) extends TypingError

/*
  TODO:

  * [X] Property Lookup
  * [ ] List literals
  * [ ] Some basic literals
  * [ ] Stuff which messes with scope
  * [ ] Function application, esp. considering overloading
  * [ ] Some operators: +, [], unary minus, AND
  *
  * [ ] Dealing with same expression in multiple scopes
  * [ ] Make sure to always infer all implied labels
  * [ ] Actually using the schema to get list of slots
 */
case class SchemaTyper(schema: Schema) {
  def infer(expr: Expression, tr: TypingResult): TypingResult = {
    tr.bind { tc: TypeContext =>
      expr match {
        case Property(v: Variable, PropertyKeyName(name)) =>
          tc.variableType(v) {
            case CTNode(labels) =>
              val keys = labels.map(schema.nodeKeys).reduce(_ ++ _)
              tc.updateType(expr -> keys(name))

            case CTRelationship(types) =>
              val keys = types.map(schema.relationshipKeys).reduce(_ ++ _)
              tc.updateType(expr -> keys(name))

            case _ => tc
          }

        case _ =>
          tc
      }
    }
  }
}

sealed trait TypingResult {
  def bind(f: TypeContext => TypingResult): TypingResult

  def inferLabels(schema: Schema): TypingResult = ???
}

final case class TypingFailed(errors: Seq[TypingError]) extends TypingResult {
  override def bind(f: TypeContext => TypingResult): TypingResult = this
}

object TypeContext {
  val empty = TypeContext(Map.empty)
}

final case class TypeContext(typeTable: Map[Expression, CypherType]) extends TypingResult {
  override def bind(f: TypeContext => TypingResult): TypingResult = f(this)

  def updateType(update: (Expression, CypherType)) = {
    val (k, v) = update
    copy(typeTable.updated(k, v))
  }

  def variableType(v: Variable)(f: CypherType => TypingResult): TypingResult = typeTable.get(v) match {
    case Some(typ) => f(typ)
    case None      => TypingFailed(Seq(MissingVariable(v)))
  }
}


  /*

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

    copy(typeTable = things.flatten.toMap)
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

object TypeContext {

  def init(statement: Statement, schema: Schema): TypeContext = {
    TypeContext(statement, schema).buildPatternTypeTable()
  }

}

  */
