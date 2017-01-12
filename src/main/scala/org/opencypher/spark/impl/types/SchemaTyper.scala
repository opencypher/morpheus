package org.opencypher.spark.impl.types

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types._

/*
  TODO:

  * [X] Property Lookup
  * [X] Some basic literals
  * [X] List literals
  * [X] Function application, esp. considering overloading
  * [ ] Some operators: +, [], unary minus, AND
  * [ ] Stuff which messes with scope
  *
  * [ ] Dealing with same expression in multiple scopes
  * [ ] Make sure to always infer all implied labels
  * [ ] Actually using the schema to get list of slots
 */
object CypherTypeExtras {
  implicit class RichCypherType(left: CypherType) {
    def couldBe(right: CypherType): Boolean = {
      left.superTypeOf(right).maybeTrue || left.subTypeOf(right).maybeTrue
    }

    def addJoin(right: CypherType): CypherType = (left, right) match {
      case (CTInteger, CTFloat) => CTFloat
      case (CTFloat, CTInteger) => CTFloat
      case (CTString, _) if right.subTypeOf(CTNumber).maybeTrue => CTString
      case (_, CTString) if left.subTypeOf(CTNumber).maybeTrue => CTString
      case _                    => left join right
    }
  }


  implicit class RichCTList(left: CTList) {

    def listConcatJoin(right: CypherType): CTList = (left, right) match {
      case (CTList(lInner), CTList(rInner)) => CTList(lInner join rInner)
      case (CTList(CTString), CTString) => left
      case (CTList(CTVoid), _) => CTList(right)
      case _ => ???
//      case (CTString, CTList(CTString)) => CTList(CTString)
//      case (CTList(CTString), CTString) => CTList(CTString)
//      case (CTList(inner), _) if (right subTypeOf inner).maybeTrue => CTList(inner)
//      case (_, CTList(inner)) if (left subTypeOf inner).maybeTrue => CTList(inner)
//      case (CTList(leftInner), CTList(rightInner)) => CTList(leftInner coerceOrJoin rightInner)
//      case (CTList(inner), _) if inner == CTVoid => CTList(right)
//      case (_, CTList(inner)) if inner == CTVoid => CTList(left)
    }
  }
}

case class SchemaTyper(schema: Schema) {
  import CypherTypeExtras._

  def infer(expr: Expression, tr: TypingResult): TypingResult = {
    tr.bind { tc0: TypeContext =>
      expr match {

        case Add(lhs, rhs) =>
          infer(lhs, tc0).bind(infer(rhs, _)).bind { tc1 =>
            tc1.typeOf(lhs, rhs) {
              case (lhsType: CTList, rhsType) =>
                tc1.updateType(expr -> (lhsType listConcatJoin rhsType))
              case (lhsType, rhsType: CTList) =>
                tc1.updateType(expr -> (rhsType listConcatJoin lhsType))
              case (lhsType, rhsType) =>
                tc1.updateType(expr -> (lhsType addJoin rhsType))
            }
          }

        case ContainerIndex(list, _) =>
          infer(list, tc0).bind { tc1 =>
            tc1.typeTable(list) match {
              case CTList(inner) =>
                tc1.updateType(expr -> inner)
              case x => throw new IllegalStateException(s"Indexing on a non-list: $x")
            }
          }

        case invocation: FunctionInvocation =>
          invocation.function match {
            case f: SimpleTypedFunction =>
              val args = invocation.args
              args.foldLeft[TypingResult](tc0) {
                case (innerTc, arg) => infer(arg, innerTc)
              }.bind { tc1: TypeContext =>
                tc1.typeOf(args) { argTypes =>

                  // TODO: Failing
                  // find all output types for which the whole signature is a match given args
//                  f.signatures.map(toCosTypes)
                  val possibleOutputs = f.signatures.collect {
                    case sig
                      if argTypes.zip(sig.argumentTypes.map(toCosTypes)).forall {
                        case (given, declared) => given.couldBe(declared)
                      } =>
                        toCosTypes(sig.outputType)
                  }
                  val output = possibleOutputs.reduce(_ join _)
                  val nullableArg = argTypes.exists(_.isNullable)
                  val exprType = if (nullableArg) output.nullable else output

                  tc1.updateType(expr -> exprType)
                }
              }

            case _ =>
              ???
          }

        case _: SignedDecimalIntegerLiteral =>
          tc0.updateType(expr -> CTInteger)

        case _: DecimalDoubleLiteral =>
          tc0.updateType(expr -> CTFloat)

        case _: BooleanLiteral =>
          tc0.updateType(expr -> CTBoolean)

        case _: StringLiteral =>
          tc0.updateType(expr -> CTString)

        case _: Null =>
          tc0.updateType(expr -> CTNull)

        case ListLiteral(elts) =>
          elts.foldLeft[TypingResult](tc0) {
            case (innerTc, elt) => infer(elt, innerTc)
          }
          .bind { tc1: TypeContext =>
            val optEltType = elts.foldLeft[Option[CypherType]](None) {
              case (None, elt) => tc1.typeTable.get(elt)
              case (Some(accType), elt) => Some(tc1.joinType(accType, elt))
            }
            val eltType = optEltType.getOrElse(CTVoid)
            tc1.updateType(expr -> CTList(eltType))
          }

        case Property(v: Variable, PropertyKeyName(name)) =>
          tc0.typeOf(v) {
            case CTNode(labels) =>
              val keys = labels.map(schema.nodeKeys).reduce(_ ++ _)
              tc0.updateType(expr -> keys(name))

            case CTRelationship(types) =>
              val keys = types.map(schema.relationshipKeys).reduce(_ ++ _)
              tc0.updateType(expr -> keys(name))

            case _ => tc0
          }

        case x => throw new UnsupportedOperationException(s"Don't know how to type $x")
      }
    }
  }
}
