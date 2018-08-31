/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.api.exception

import org.opencypher.okapi.api.exception.CypherException.{ErrorDetails, ErrorPhase, ErrorType}

/**
  * Cypher exceptions are thrown due to the reasons specified by the Cypher TCK and follow the specified format.
  */
// TODO: Make sure all Cypher-related exceptions we throw are instances of this
// TODO: Make sure all other exceptions are instances of another trait in this package
abstract class CypherException(val errorType: ErrorType, val phase: ErrorPhase, val detail: ErrorDetails)
  extends RuntimeException(s"$errorType during $phase. Details: $detail") with Serializable

object CypherException {

  sealed trait ErrorType

  /**
    * Possible error types as specified by the TCK.
    */
  object ErrorType {

    case object SyntaxError extends ErrorType

    case object ParameterMissing extends ErrorType

    case object ConstraintVerificationFailed extends ErrorType

    //TODO: Validation/verification are too similar. Fix in TCK?
    case object ConstraintValidationFailed extends ErrorType

    case object EntityNotFound extends ErrorType

    case object PropertyNotFound extends ErrorType

    case object LabelNotFound extends ErrorType

    case object TypeError extends ErrorType

    case object ArgumentError extends ErrorType

    case object ArithmeticError extends ErrorType

    case object ProcedureError extends ErrorType

  }

  sealed trait ErrorPhase

  /**
    * Possible error phases as specified by the TCK.
    */
  object ErrorPhase {

    case object Runtime extends ErrorPhase {
      override def toString: String = "runtime"
    }

    case object CompileTime extends ErrorPhase {
      override def toString: String = "compile time"
    }

  }

  sealed trait ErrorDetails {
    def message: String
    override def toString: String = s"${getClass.getSimpleName} $message"
  }

  case class ParsingError(message: String) extends ErrorDetails

  case class InvalidElementAccess(message: String) extends ErrorDetails

  case class MapElementAccessByNonString(message: String) extends ErrorDetails

  case class ListElementAccessByNonInteger(message: String) extends ErrorDetails

  case class NestedAggregation(message: String) extends ErrorDetails

  case class NegativeIntegerArgument(message: String) extends ErrorDetails

  case class DeleteConnectedNode(message: String) extends ErrorDetails

  case class RequiresDirectedRelationship(message: String) extends ErrorDetails

  case class InvalidRelationshipPattern(message: String) extends ErrorDetails

  case class VariableAlreadyBound(message: String) extends ErrorDetails

  case class InvalidArgumentType(message: String) extends ErrorDetails

  case class InvalidArgumentValue(message: String) extends ErrorDetails

  case class NumberOutOfRange(message: String) extends ErrorDetails

  case class UndefinedVariable(message: String) extends ErrorDetails

  case class VariableTypeConflict(message: String) extends ErrorDetails

  case class RelationshipUniquenessViolation(message: String) extends ErrorDetails

  case class CreatingVarLength(message: String) extends ErrorDetails

  case class InvalidParameterUse(message: String) extends ErrorDetails

  case class InvalidClauseComposition(message: String) extends ErrorDetails

  case class FloatingPointOverflow(message: String) extends ErrorDetails

  case class PropertyAccessOnNonMap(message: String) extends ErrorDetails

  case class InvalidArgumentExpression(message: String) extends ErrorDetails

  case class NonConstantExpression(message: String) extends ErrorDetails

  case class NoSingleRelationshipType(message: String) extends ErrorDetails

  case class InvalidAggregation(message: String) extends ErrorDetails

  case class UnknownFunction(message: String) extends ErrorDetails

  case class InvalidNumberLiteral(message: String) extends ErrorDetails

  case class MergeReadOwnWrites(message: String) extends ErrorDetails

  case class NoExpressionAlias(message: String) extends ErrorDetails

  case class DifferentColumnsInUnion(message: String) extends ErrorDetails

  case class InvalidDelete(message: String) extends ErrorDetails

  case class InvalidPropertyType(message: String) extends ErrorDetails

  case class ColumnNameConflict(message: String) extends ErrorDetails

  case class NoVariablesInScope(message: String) extends ErrorDetails

  case class DeletedEntityAccess(message: String) extends ErrorDetails

  case class InvalidArgumentPassingMode(message: String) extends ErrorDetails

  case class InvalidNumberOfArguments(message: String) extends ErrorDetails

  case class MissingParameter(message: String) extends ErrorDetails

  case class ProcedureNotFound(message: String) extends ErrorDetails

}
