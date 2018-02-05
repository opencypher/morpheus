/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.impl.spark

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.opencypher.caps.api.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException}
import org.opencypher.caps.api.types.{CTAny, CTList, CTNode, CTString}
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.impl.record.RecordHeader
import org.opencypher.caps.impl.spark.Udfs._
import org.opencypher.caps.impl.spark.convert.SparkUtils._
import org.opencypher.caps.impl.spark.physical.RuntimeContext
import org.opencypher.caps.impl.spark.physical.operators.PhysicalOperator.columnName
import org.opencypher.caps.ir.api.expr._

object SparkSQLExprMapper {

  private def verifyExpression(header: RecordHeader, expr: Expr) = {
    val slots = header.slotsFor(expr)

    if (slots.isEmpty) {
      throw IllegalStateException(s"No slot for expression $expr")
    } else if (slots.size > 1 && !expr.isInstanceOf[Var]) {
      throw NotImplementedException("Support for multi-column expressions")
    }
  }

  private def getColumn(expr: Expr, header: RecordHeader, dataFrame: DataFrame)(
    implicit context: RuntimeContext): Column = {
    expr match {
      case p@Param(name) if p.cypherType.subTypeOf(CTList(CTAny)).maybeTrue =>
        context.parameters(name) match {
          case CypherList(l) => functions.array(l.unwrap.map(functions.lit(_)): _*)
          case notAList => throw IllegalArgumentException("a Cypher list", notAList)
        }
      case Param(name) =>
        functions.lit(context.parameters(name).unwrap)
      case ListLit(exprs) =>
        val cols = exprs.map(getColumn(_, header, dataFrame))
        functions.array(cols: _*)
      case l: Lit[_] =>
        functions.lit(l.v)
      case _ =>
        verifyExpression(header, expr)
        val slot = header.slotsFor(expr).head

        val columns = dataFrame.columns.toSet
        val colName = columnName(slot)

        if (columns.contains(colName)) {
          dataFrame.col(colName)
        } else {
          functions.lit(null)
        }
    }
  }

  object asSparkSQLExpr {

    /**
      * Attempts to create a Spark SQL expression from the CAPS expression.
      *
      * @param header  the header of the CAPSRecords in which the expression should be evaluated.
      * @param expr    the expression to be evaluated.
      * @param df      the dataframe containing the data over which the expression should be evaluated.
      * @param context context with helper functions, such as column names.
      * @return Some Spark SQL expression if the input was mappable, otherwise None.
      */
    def apply(header: RecordHeader, expr: Expr, df: DataFrame)(implicit context: RuntimeContext): Option[Column] =
      expr match {

        case _: Var =>
          val col = getColumn(expr, header, df)
          Some(col)

        case _: Param =>
          Some(getColumn(expr, header, df))

        case ListLit(exprs) =>
          val cols = exprs.map(asSparkSQLExpr(header, _, df).get)
          Some(functions.array(cols: _*))

        case _: Lit[_] =>
          Some(getColumn(expr, header, df))

        case _: Property =>
          Some(getColumn(expr, header, df))

        // predicates
        case Equals(e1, e2) =>
          val lCol = getColumn(e1, header, df)
          val rCol = getColumn(e2, header, df)
          Some(lCol === rCol)

        case Not(e) =>
          apply(header, e, df) match {
            case Some(res) => Some(!res)
            case _ => throw NotImplementedException(s"Support for expression $e")
          }

        case IsNull(e) =>
          val col = getColumn(e, header, df)
          Some(col.isNull)

        case IsNotNull(e) =>
          val col = getColumn(e, header, df)
          Some(col.isNotNull)

        case s: Size =>
          verifyExpression(header, expr)

          val col = getColumn(s.expr, header, df)
          val computedSize = s.expr.cypherType match {
            case CTString => udf((s: String) => s.length.toLong, LongType)(col)
            case _: CTList => functions.size(col).cast(LongType)
            case other => throw NotImplementedException(s"size() on type $other")
          }
          Some(computedSize)

        case Ands(exprs) =>
          val cols = exprs.map(asSparkSQLExpr(header, _, df))
          if (cols.contains(None)) None
          else Some(cols.flatten.foldLeft(functions.lit(true))(_ && _))

        case Ors(exprs) =>
          val cols = exprs.map(asSparkSQLExpr(header, _, df))
          if (cols.contains(None)) None
          else Some(cols.flatten.foldLeft(functions.lit(false))(_ || _))

        case In(lhs, rhs) =>
          verifyExpression(header, expr)

          val lhsColumn = getColumn(lhs, header, df)
          val rhsColumn = getColumn(rhs, header, df)

          // if we were able to store the list as a Spark array, we could do
          // new Column(ArrayContains(lhsColumn.expr, rhsColumn.expr))
          // and avoid UDF

          val inPred = udf(Udfs.in _, BooleanType)(lhsColumn, rhsColumn)

          Some(inPred)

        case HasType(rel, relType) =>
          val col = getColumn(Type(rel)(), header, df)
          Some(col === relType.name)

        case h: HasLabel =>
          Some(getColumn(h, header, df)) // it's a boolean column

        case LessThan(lhs, rhs) =>
          val l = getColumn(lhs, header, df)
          val r = getColumn(rhs, header, df)

          Some(l < r)

        case LessThanOrEqual(lhs, rhs) =>
          val l = getColumn(lhs, header, df)
          val r = getColumn(rhs, header, df)

          Some(l <= r)

        case GreaterThanOrEqual(lhs, rhs) =>
          val l = getColumn(lhs, header, df)
          val r = getColumn(rhs, header, df)

          Some(l >= r)

        case GreaterThan(lhs, rhs) =>
          val l = getColumn(lhs, header, df)
          val r = getColumn(rhs, header, df)

          Some(l > r)

        // Arithmetics
        case add: Add =>
          verifyExpression(header, expr)

          val lhsColumn = getColumn(add.lhs, header, df)
          val rhsColumn = getColumn(add.rhs, header, df)
          Some(lhsColumn + rhsColumn)

        case sub: Subtract =>
          verifyExpression(header, expr)

          val lhsColumn = getColumn(sub.lhs, header, df)
          val rhsColumn = getColumn(sub.rhs, header, df)
          Some(lhsColumn - rhsColumn)

        case mul: Multiply =>
          verifyExpression(header, expr)

          val lhsColumn = getColumn(mul.lhs, header, df)
          val rhsColumn = getColumn(mul.rhs, header, df)
          Some(lhsColumn * rhsColumn)

        case div: Divide =>
          verifyExpression(header, expr)

          val lhsColumn = getColumn(div.lhs, header, df)
          val rhsColumn = getColumn(div.rhs, header, df)
          Some((lhsColumn / rhsColumn).cast(toSparkType(div.cypherType)))

        // Functions
        case e: Exists =>
          val column = getColumn(e.expr, header, df)
          Some(column.isNotNull)

        case id: Id =>
          verifyExpression(header, expr)

          val column = getColumn(id.expr, header, df)
          Some(column)

        case labels: Labels =>
          verifyExpression(header, expr)

          val node = Var(columnName(header.slotsFor(labels.expr).head))(CTNode)
          val labelExprs = header.labels(node)
          val labelColumns = labelExprs.map(getColumn(_, header, df))
          val labelNames = labelExprs.map(_.label)
          val labelsUDF = udf(getNodeLabels(labelNames), ArrayType(StringType, containsNull = false))
          Some(labelsUDF(functions.array(labelColumns: _*)))

        case keys: Keys =>
          verifyExpression(header, expr)

          val node = Var(columnName(header.slotsFor(keys.expr).head))(CTNode)
          val propertyExprs = header.properties(node)
          val propertyColumns = propertyExprs.map(getColumn(_, header, df))
          val keyNames = propertyExprs.map(_.key.name)
          val keysUDF = udf(getNodeKeys(keyNames), ArrayType(StringType, containsNull = false))
          Some(keysUDF(functions.array(propertyColumns: _*)))

        case Type(inner) =>
          verifyExpression(header, expr)

          inner match {
            case v: Var =>
              val typeSlot = header.typeSlot(v)
              val typeCol = df.col(columnName(typeSlot))
              Some(typeCol)

            case _ =>
              throw NotImplementedException("type() of non-variables")
          }

        case StartNodeFunction(e) =>
          verifyExpression(header, expr)
          val rel = Var(columnName(header.slotsFor(e).head))(CTNode)
          Some(getColumn(header.sourceNodeSlot(rel).content.key, header, df))

        case EndNodeFunction(e) =>
          verifyExpression(header, expr)
          val rel = Var(columnName(header.slotsFor(e).head))(CTNode)
          Some(getColumn(header.targetNodeSlot(rel).content.key, header, df))

        case ToFloat(e) =>
          verifyExpression(header, expr)
          Some(getColumn(e, header, df).cast(DoubleType))

        // Pattern Predicate
        case ep: ExistsPatternExpr =>
          Some(getColumn(ep.targetField, header, df))

        case c: Coalesce =>
          val columns = c.exprs.map(e => getColumn(e, header, df))
          Some(functions.coalesce(columns: _*))

        case _ =>
          None
      }
  }

}
