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
package org.opencypher.okapi.impl.spark

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException}
import org.opencypher.okapi.impl.table.RecordHeader
import org.opencypher.okapi.impl.spark.CAPSFunctions._
import org.opencypher.okapi.impl.spark.DataFrameOps._
import org.opencypher.okapi.impl.spark.physical.CAPSRuntimeContext
import org.opencypher.okapi.impl.spark.physical.operators.CAPSPhysicalOperator.columnName
import org.opencypher.okapi.ir.api.expr._

object SparkSQLExprMapper {

  implicit class RichExpression(expr: Expr) {

    def verify(implicit header: RecordHeader): Unit = {
      if (header.slotsFor(expr).isEmpty) throw IllegalStateException(s"No slot for expression $expr")
    }

    /**
      * This is possible without violating Cypher semantics because
      *   - Spark SQL returns null when comparing across types (from initial investigation)
      *   - We never have multiple types per column in CAPS (yet)
      */
    def compare(comparator: Column => (Column => Column), lhs: Expr, rhs: Expr)
      (implicit header: RecordHeader, df: DataFrame, context: CAPSRuntimeContext): Column = {
      comparator(lhs.asSparkSQLExpr)(rhs.asSparkSQLExpr)
    }

    def lt(c: Column): Column => Column = c < _

    def lteq(c: Column): Column => Column = c <= _

    def gt(c: Column): Column => Column = c > _

    def gteq(c: Column): Column => Column = c >= _

    /**
      * Attempts to create a Spark SQL expression from the CAPS expression.
      *
      * @param header  the header of the CAPSRecords in which the expression should be evaluated.
      * @param df      the dataframe containing the data over which the expression should be evaluated.
      * @param context context with helper functions, such as column names.
      * @return Some Spark SQL expression if the input was mappable, otherwise None.
      */
    def asSparkSQLExpr(implicit header: RecordHeader, df: DataFrame, context: CAPSRuntimeContext): Column = {

      expr match {

        // context based lookups
        case p@Param(name) if p.cypherType.subTypeOf(CTList(CTAny)).maybeTrue =>
          context.parameters(name) match {
            case CypherList(l) => functions.array(l.unwrap.map(functions.lit): _*)
            case notAList => throw IllegalArgumentException("a Cypher list", notAList)
          }
        case Param(name) =>
          functions.lit(context.parameters(name).unwrap)

        // direct column lookup
        case _: Var | _: Param | _: Property | _: HasLabel | _: StartNode | _: EndNode =>
          verify

          val slot = header.slotsFor(expr).head

          val columns = df.columns.toSet
          val colName = columnName(slot)

          if (columns.contains(colName)) {
            df.col(colName)
          } else {
            functions.lit(null)
          }

        // Literals
        case ListLit(exprs) =>
          functions.array(exprs.map(_.asSparkSQLExpr): _*)

        case l: Lit[_] => functions.lit(l.v)

        // predicates
        case Equals(e1, e2) => e1.asSparkSQLExpr === e2.asSparkSQLExpr
        case Not(e) => !e.asSparkSQLExpr
        case IsNull(e) => e.asSparkSQLExpr.isNull
        case IsNotNull(e) => e.asSparkSQLExpr.isNotNull
        case Size(e) =>
          val col = e.asSparkSQLExpr
          e.cypherType match {
            case CTString => functions.length(col).cast(LongType)
            case _: CTList => functions.size(col).cast(LongType)
            case other => throw NotImplementedException(s"size() on values of type $other")
          }

        case Ands(exprs) =>
          exprs.map(_.asSparkSQLExpr).foldLeft(functions.lit(true))(_ && _)

        case Ors(exprs) =>
          exprs.map(_.asSparkSQLExpr).foldLeft(functions.lit(false))(_ || _)

        case In(lhs, rhs) =>
          val element = lhs.asSparkSQLExpr
          val array = rhs.asSparkSQLExpr
          array_contains(array, element)

        case HasType(rel, relType) =>
          Type(rel)().asSparkSQLExpr === relType.name

        case LessThan(lhs, rhs) => compare(lt, lhs, rhs)
        case LessThanOrEqual(lhs, rhs) => compare(lteq, lhs, rhs)
        case GreaterThanOrEqual(lhs, rhs) => compare(gteq, lhs, rhs)
        case GreaterThan(lhs, rhs) => compare(gt, lhs, rhs)

        // Arithmetics
        case Add(lhs, rhs) => lhs.asSparkSQLExpr + rhs.asSparkSQLExpr
        case Subtract(lhs, rhs) => lhs.asSparkSQLExpr - rhs.asSparkSQLExpr
        case Multiply(lhs, rhs) => lhs.asSparkSQLExpr * rhs.asSparkSQLExpr
        case div@Divide(lhs, rhs) => (lhs.asSparkSQLExpr / rhs.asSparkSQLExpr).cast(toSparkType(div.cypherType))

        // Functions
        case Exists(e) => e.asSparkSQLExpr.isNotNull
        case Id(e) => e.asSparkSQLExpr
        case Labels(e) =>
          val node = Var(columnName(header.slotsFor(e).head))(CTNode)
          val labelExprs = header.labels(node)
          val labelColumns = labelExprs.map(_.asSparkSQLExpr)
          val labelNames = labelExprs.map(_.label.name)
          val booleanLabelFlagColumn = functions.array(labelColumns: _*)
          get_node_labels(labelNames)(booleanLabelFlagColumn)

        case Keys(e) =>
          val node = Var(columnName(header.slotsFor(e).head))(CTNode)
          val propertyExprs = header.properties(node)
          val propertyColumns = propertyExprs.map(_.asSparkSQLExpr)
          val keyNames = propertyExprs.map(_.key.name)
          val valuesColumn = functions.array(propertyColumns: _*)
          get_property_keys(keyNames)(valuesColumn)

        case Type(inner) =>
          inner match {
            case v: Var =>
              val typeSlot = header.typeSlot(v)
              val typeCol = df.col(columnName(typeSlot))
              typeCol
            case _ =>
              throw NotImplementedException(s"Inner expression $inner of $expr is not yet supported (only variables)")
          }

        case StartNodeFunction(e) =>
          val rel = Var(columnName(header.slotsFor(e).head))(CTNode)
          header.sourceNodeSlot(rel).content.key.asSparkSQLExpr

        case EndNodeFunction(e) =>
          val rel = Var(columnName(header.slotsFor(e).head))(CTNode)
          header.targetNodeSlot(rel).content.key.asSparkSQLExpr

        case ToFloat(e) => e.asSparkSQLExpr.cast(DoubleType)

        // Pattern Predicate
        case ep: ExistsPatternExpr => ep.targetField.asSparkSQLExpr

        case Coalesce(es) =>
          val columns = es.map(_.asSparkSQLExpr)
          functions.coalesce(columns: _*)

        case c: CaseExpr =>
          val alternatives = c.alternatives.map {
            case (predicate, action) => functions.when(predicate.asSparkSQLExpr, action.asSparkSQLExpr)
          }

          val alternativesWithDefault = c.default match {
            case Some(inner) => alternatives :+ inner.asSparkSQLExpr
            case None => alternatives
          }

          val reversedColumns = alternativesWithDefault.reverse

          val caseColumn = reversedColumns.tail.foldLeft(reversedColumns.head) {
            case (tmpCol, whenCol) => whenCol.otherwise(tmpCol)
          }
          caseColumn

        case _ =>
          throw NotImplementedException(s"No support for converting Cypher expression $expr to a Spark SQL expression")
      }
    }
  }

}
