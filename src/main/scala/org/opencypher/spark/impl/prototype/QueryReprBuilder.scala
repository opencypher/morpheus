package org.opencypher.spark.impl.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.opencypher.spark.api.CypherType

import scala.collection.mutable

object QueryReprBuilder {
  def from(s: Statement, q: String, tokenDefs: TokenDefs, params: Set[String]): QueryRepresentation = {
    val builder = new QueryReprBuilder(q, tokenDefs, params)
    s match {
      case Query(_, part) => part match {
        case SingleQuery(clauses) => clauses.foreach(builder.add)
      }
      case _ => ???
    }

    builder.build()
  }
}

class QueryReprBuilder(query: String, tokenDefs: TokenDefs, paramNames: Set[String]) {
  val predicates: mutable.Set[Expr] = mutable.Set.empty
  val nodes: mutable.Set[String] = mutable.Set.empty
  val rels: mutable.Set[Expr] = mutable.Set.empty
  val returns: mutable.Set[ast.Expression] = mutable.Set.empty

  val returnColumns = mutable.Seq.empty[(String, CypherType)]



  def add(c: Clause): Unit = {
    c match {
      case Match(_, pattern, _, where) =>
        add(pattern)
        val preds = convertWhere(where)
        new BasicBlockDef {
          override def predicates = preds
          override def given = ???
          override def outputs = ???
          override def dependencies = ???
          override def inputs = ???
          override def blockType = ???
        }
      case Return(_, ReturnItems(_, items), _, _, _, _) =>
        items.foreach(addReturn)
    }
  }

  private def convertWhere(where: Option[Where]): Set[Predicate] = where match {
    case Some(Where(expr)) => convert(expr) match {
      case Ands(exprs) => exprs.map(Predicate)
      case e => Set(Predicate(e))
    }
    case None => Set.empty
  }

  def addReturn(r: ReturnItem) = {
    r match {
      case AliasedReturnItem(expr, variable) => returns.add(expr)
      case UnaliasedReturnItem(expr, text) => returns.add(expr)
    }
  }

  def add(p: Pattern): Unit = {

  }

  private def convert(e: ast.Expression) = new ExpressionConverter(tokenDefs).convert(e)

  def addPredicate(e: ast.Expression): Unit = {
    convert(e)
  }

  def build(): QueryRepresentation = {

    new QueryRepresentation {
      override def root = ???
      override def returns = ???
      override def cypherQuery = query
      override def cypherVersion = "SparkCypher 1"

      override def params: Map[Param, String] = paramNames.map(s => ParameterNameGenerator.generate(s) -> s).toMap
    }
  }
}

object ParameterNameGenerator {
  def generate(n: String): Param = Param(n)
}
