/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.impl

import org.apache.spark.sql.functions.{array_contains => _, translate => _, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception._
import org.opencypher.okapi.impl.temporal.TemporalTypesHelper._
import org.opencypher.okapi.impl.temporal.{Duration => DurationValue}
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.impl.CAPSFunctions._
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.expressions.AddPrefix._
import org.opencypher.spark.impl.expressions.EncodeLong._
import org.opencypher.spark.impl.temporal.SparkTemporalHelpers._
import org.opencypher.spark.impl.temporal.TemporalUdfs

final case class SparkSQLMappingException(msg: String) extends InternalException(msg)

object SparkSQLExprMapper {

  implicit class RichExpression(expr: Expr) {

    /**
      * Attempts to create a Spark SQL expression from the CAPS expression.
      *
      * @param header     the header of the CAPSRecords in which the expression should be evaluated.
      * @param df         the dataframe containing the data over which the expression should be evaluated.
      * @param parameters query parameters
      * @return Some Spark SQL expression if the input was mappable, otherwise None.
      */
    def asSparkSQLExpr(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {
      val outCol = expr match {
        // Evaluate based on already present data; no recursion
        case _: Var | _: HasLabel | _: HasType | _: StartNode | _: EndNode => column_for(expr)
        // Evaluate bottom-up
        case _ => null_safe_conversion(expr)(convert)
      }
      header.getColumn(expr) match {
        case None => outCol
        case Some(colName) => outCol.as(colName)
      }
    }

    private def convert(convertedChildren: Seq[Column])
      (implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {

      def c0: Column = convertedChildren.head

      def c1: Column = convertedChildren(1)

      def c2: Column = convertedChildren(2)

      expr match {
        case _: ListLit => array(convertedChildren: _*)
        case l: Lit[_] => lit(l.v)
        case _: AliasExpr => c0
        case Param(name) => parameters(name).toSparkLiteral

        // Predicates
        case _: Equals => c0 === c1
        case _: Not => !c0
        case Size(e) => {
          e.cypherType match {
            case CTString => length(c0)
            case _ => size(c0) // it's a list
          }
        }.cast(LongType)
        case _: Ands => convertedChildren.foldLeft(TRUE_LIT)(_ && _)
        case _: Ors => convertedChildren.foldLeft(FALSE_LIT)(_ || _)
        case _: IsNull => c0.isNull
        case _: IsNotNull => c0.isNotNull
        case _: Exists => c0.isNotNull
        case _: LessThan => c0 < c1
        case _: LessThanOrEqual => c0 <= c1
        case _: GreaterThanOrEqual => c0 >= c1
        case _: GreaterThan => c0 > c1

        case _: StartsWith => c0.startsWith(c1)
        case _: EndsWith => c0.endsWith(c1)
        case _: Contains => c0.contains(c1)
        case _: RegexMatch => regex_match(c0, c1)

        // Other
        case Explode(list) => list.cypherType match {
          case CTNull => explode(NULL_LIT.cast(ArrayType(NullType)))
          case _ => explode(c0)
        }

        case Property(e, PropertyKey(key)) =>
          // Convert property lookups into separate specific lookups instead of overloading
          e.cypherType.material match {
            case CTMap(inner) => if (inner.keySet.contains(key)) c0.getField(key) else NULL_LIT
            case CTDate => temporalAccessor[java.sql.Date](c0, key)
            case CTLocalDateTime => temporalAccessor[java.sql.Timestamp](c0, key)
            case CTDuration => TemporalUdfs.durationAccessor(key.toLowerCase).apply(c0)
            case _ =>
              // TODO: Investigate this case
              if (!header.contains(expr)) {
                NULL_LIT
              } else {
                column_for(expr)
              }
          }
        case LocalDateTime(dateExpr) =>
          // TODO: Move code outside of expr mapper
          dateExpr match {
            case Some(e) =>
              val localDateTimeValue = resolveTemporalArgument(e)
                .map(parseLocalDateTime)
                .map(java.sql.Timestamp.valueOf)
                .map {
                  case ts if ts.getNanos % 1000 == 0 => ts
                  case _ => throw IllegalStateException("Spark does not support nanosecond resolution in 'localdatetime'")
                }
                .orNull

              lit(localDateTimeValue).cast(DataTypes.TimestampType)
            case None => current_timestamp()
          }

        case Date(dateExpr) =>
          // TODO: Move code outside of expr mapper
          dateExpr match {
            case Some(e) =>
              val dateValue = resolveTemporalArgument(e)
                .map(parseDate)
                .map(java.sql.Date.valueOf)
                .orNull

              lit(dateValue).cast(DataTypes.DateType)
            case None => current_timestamp()
          }

        case Duration(durationExpr) =>
          // TODO: Move code outside of expr mapper
          val durationValue = resolveTemporalArgument(durationExpr).map {
            case Left(m) => DurationValue(m.mapValues(_.toLong)).toCalendarInterval
            case Right(s) => DurationValue.parse(s).toCalendarInterval
          }.orNull
          lit(durationValue)

        case In(lhs, rhs) => rhs.cypherType.material match {
          case CTList(CTVoid) => FALSE_LIT
          case CTList(inner) if inner.couldBeSameTypeAs(lhs.cypherType) => array_contains(c1, c0)
          case _ => NULL_LIT
        }

        // Arithmetic
        case Add(lhs, rhs) =>
          val lhsCT = lhs.cypherType.material
          val rhsCT = rhs.cypherType.material
          lhsCT -> rhsCT match {
            case (CTList(lhInner), CTList(rhInner)) =>
              if (lhInner.material == rhInner.material || lhInner == CTVoid || rhInner == CTVoid) {
                concat(c0, c1)
              } else {
                throw SparkSQLMappingException(s"Lists of different inner types are not supported (${lhInner.material}, ${rhInner.material})")
              }
            case (CTList(inner), nonListType) if nonListType == inner.material || inner.material == CTVoid => concat(c0, array(c1))
            case (nonListType, CTList(inner)) if inner.material == nonListType || inner.material == CTVoid => concat(array(c0), c1)
            case (CTString, _) if rhsCT.subTypeOf(CTNumber) => concat(c0, c1.cast(StringType))
            case (_, CTString) if lhsCT.subTypeOf(CTNumber) => concat(c0.cast(StringType), c1)
            case (CTString, CTString) => concat(c0, c1)
            case (CTDate, CTDuration) => TemporalUdfs.dateAdd(c0, c1)
            case _ => c0 + c1
          }

        case Subtract(lhs, rhs) if lhs.cypherType.material.subTypeOf(CTDate) && rhs.cypherType.material.subTypeOf(CTDuration) =>
          TemporalUdfs.dateSubtract(c0, c1)

        case _: Subtract => c0 - c1

        case _: Multiply => c0 * c1
        case div: Divide => (c0 / c1).cast(div.cypherType.getSparkType)

        // Id functions
        case _: Id => c0
        case PrefixId(_, prefix) => c0.addPrefix(lit(prefix))
        case ToId(e) =>
          e.cypherType.material match {
            case CTInteger => c0.encodeLongAsCAPSId
            // TODO: Remove this call; we shouldn't have nodes or rels as concrete types here
            case _: CTNode | _: CTRelationship | CTIdentity => c0
            case other => throw IllegalArgumentException("a type that may be converted to an ID", other)
          }

        // Functions
        case _: MonotonicallyIncreasingId => monotonically_increasing_id()
        case Labels(e) =>
          val possibleLabels = header.labelsFor(e.owner.get).toSeq.sortBy(_.label.name)
          val labelBooleanFlagsCol = possibleLabels.map(_.asSparkSQLExpr)
          val nodeLabels = filter_true(possibleLabels.map(_.label.name), labelBooleanFlagsCol)
          nodeLabels

        case Type(e) =>
          val possibleRelTypes = header.typesFor(e.owner.get).toSeq.sortBy(_.relType.name)
          val relTypeBooleanFlagsCol = possibleRelTypes.map(_.asSparkSQLExpr)
          val relTypes = filter_true(possibleRelTypes.map(_.relType.name), relTypeBooleanFlagsCol)
          val relType = get_array_item(relTypes, index = 0)
          relType

        case Keys(e) =>
          e.cypherType.material match {
            case _: CTNode | _: CTRelationship =>
              val possibleProperties = header.propertiesFor(e.owner.get).toSeq.sortBy(_.key.name)
              val propertyNames = possibleProperties.map(_.key.name)
              val propertyValues = possibleProperties.map(_.asSparkSQLExpr)
              filter_not_null(propertyNames, propertyValues)

            case CTMap(inner) =>
              val mapColumn = c0
              val (propertyKeys, propertyValues) = inner.keys.map { e =>
                // Whe have to make sure that every column has the same type (true or null)
                e -> when(mapColumn.getField(e).isNotNull, TRUE_LIT).otherwise(NULL_LIT)
              }.toSeq.unzip
              filter_not_null(propertyKeys, propertyValues)

            case other => throw IllegalArgumentException("an Expression with type CTNode, CTRelationship or CTMap", other)
          }

        case Properties(e) =>
          e.cypherType.material match {
            case _: CTNode | _: CTRelationship =>
              val propertyExpressions = header.propertiesFor(e.owner.get).toSeq.sortBy(_.key.name)
              val propertyColumns = propertyExpressions
                .map(propertyExpression => propertyExpression.asSparkSQLExpr.as(propertyExpression.key.name))
              create_struct(propertyColumns)
            case _: CTMap => c0
            case other =>
              throw IllegalArgumentException("a node, relationship or map", other, "Invalid input to properties function")
          }

        case StartNodeFunction(e) => header.startNodeFor(e.owner.get).asSparkSQLExpr
        case EndNodeFunction(e) => header.endNodeFor(e.owner.get).asSparkSQLExpr

        case _: ToFloat => c0.cast(DoubleType)
        case _: ToInteger => c0.cast(IntegerType)
        case _: ToString => c0.cast(StringType)
        case _: ToBoolean => c0.cast(BooleanType)

        case _: Trim => trim(c0)
        case _: LTrim => ltrim(c0)
        case _: RTrim => rtrim(c0)
        case _: ToUpper => upper(c0)
        case _: ToLower => lower(c0)

        case _: Range =>
          val stepCol = convertedChildren.applyOrElse(2, (_: Int) => ONE_LIT)
          sequence(c0, c1, stepCol)

        case _: Replace => translate(c0, c1, c2)

        case _: Substring =>
          val lengthCol = convertedChildren.applyOrElse(2, (_: Int) => length(c0) - c1)
          c0.substr(c1 + ONE_LIT, lengthCol)

        // Mathematical functions
        case E => E_LIT
        case Pi => PI_LIT

        case _: Sqrt => sqrt(c0)
        case _: Log => log(c0)
        case _: Log10 => log(10.0, c0)
        case _: Exp => exp(c0)
        case _: Abs => abs(c0)
        case _: Ceil => ceil(c0).cast(DoubleType)
        case _: Floor => floor(c0).cast(DoubleType)
        case Rand => rand()
        case _: Round => round(c0).cast(DoubleType)
        case _: Sign => signum(c0).cast(IntegerType)

        case _: Acos => acos(c0)
        case _: Asin => asin(c0)
        case _: Atan => atan(c0)
        case _: Atan2 => atan2(c0, c1)
        case _: Cos => cos(c0)
        case Cot(e) => Divide(IntegerLit(1), Tan(e))(CTFloat).asSparkSQLExpr
        case _: Degrees => degrees(c0)
        case Haversin(e) => Divide(Subtract(IntegerLit(1), Cos(e))(CTFloat), IntegerLit(2))(CTFloat).asSparkSQLExpr
        case _: Radians => radians(c0)
        case _: Sin => sin(c0)
        case _: Tan => tan(c0)

        // Time functions
        case Timestamp => current_timestamp().cast(LongType)

        // Bit operations
        case _: BitwiseAnd => c0.bitwiseAND(c1)
        case _: BitwiseOr => c0.bitwiseOR(c1)
        case ShiftLeft(_, IntegerLit(shiftBits)) => shiftLeft(c0, shiftBits.toInt)
        case ShiftRightUnsigned(_, IntegerLit(shiftBits)) => shiftRightUnsigned(c0, shiftBits.toInt)

        // Pattern Predicate
        case ep: ExistsPatternExpr => ep.targetField.asSparkSQLExpr

        case Coalesce(es) =>
          val columns = es.map(_.asSparkSQLExpr)
          coalesce(columns: _*)

        case CaseExpr(alternatives, default) =>
          val convertedAlternatives = alternatives.map {
            case (predicate, action) => when(predicate.asSparkSQLExpr, action.asSparkSQLExpr)
          }

          val alternativesWithDefault = default match {
            case Some(inner) => convertedAlternatives :+ inner.asSparkSQLExpr
            case None => convertedAlternatives
          }

          val reversedColumns = alternativesWithDefault.reverse

          val caseColumn = reversedColumns.tail.foldLeft(reversedColumns.head) {
            case (tmpCol, whenCol) => whenCol.otherwise(tmpCol)
          }
          caseColumn

        case ContainerIndex(container, index) =>
          val indexCol = index.asSparkSQLExpr
          val containerCol = container.asSparkSQLExpr

          container.cypherType.material match {
            case _: CTList | _: CTMap => containerCol.get(indexCol)
            case other => throw NotImplementedException(s"Accessing $other by index is not supported")
          }

        case MapExpression(items) => expr.cypherType.material match {
          case CTMap(_) =>
            val innerColumns = items.map {
              case (key, innerExpr) => innerExpr.asSparkSQLExpr.as(key)
            }.toSeq
            create_struct(innerColumns)
          case other => throw IllegalArgumentException("an expression of type CTMap", other)
        }

        // Aggregators
        case Count(_, distinct) =>
          if (distinct) countDistinct(c0)
          else count(c0)

        case Collect(_, distinct) =>
          if (distinct) collect_set(c0)
          else collect_list(c0)

        case CountStar => count(ONE_LIT)
        case _: Avg => avg(c0) //.cast(cypherType.getSparkType)
        case _: Max => max(c0)
        case _: Min => min(c0)
        case _: Sum => sum(c0)

        case _ =>
          throw NotImplementedException(s"No support for converting Cypher expression $expr to a Spark SQL expression")
      }
    }

  }

}
