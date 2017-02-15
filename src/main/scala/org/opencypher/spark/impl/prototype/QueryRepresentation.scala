package org.opencypher.spark.impl.prototype

import org.opencypher.spark.api.CypherType

import scala.collection.immutable.SortedSet

trait QueryRepresentation {
  def cypherQuery: String
  def cypherVersion: String
  def returns: SortedSet[(String, CypherType)]
  def root: RootBlock
}

trait RootBlock {
  def fields: Set[Field]

  def labels: Map[LabelRef, LabelDef]
  def relTypes: Map[RelTypeRef, RelTypeDef]
  def propertyKeys: Map[PropertyKeyRef, PropertyKeyDef]

  def blocks: Map[BlockRef, BlockDef]

  def solve: BlockRef
}

trait BlockDef {
  def blockType: BlockType
  def isLeaf = dependencies.isEmpty
  def dependencies: Seq[BlockRef]

  def inputs: Seq[Field]
  def outputs: Seq[Field]
}

trait BasicBlockDef extends BlockDef {
  def given: Set[AnyEntity]
  def predicates: Set[Predicate]
}

final case class BlockRef(name: String)

final case class Field(name: String)

sealed trait TokenDef {
  def handle: String
  def name: String
}

sealed trait TokenRef[D <: TokenDef] {
  def id: Int
}

final case class LabelDef(handle: String, name: String) extends TokenDef
final case class LabelRef(id: Int) extends TokenRef[LabelDef]

final case class PropertyKeyDef(handle: String, name: String) extends TokenDef
final case class PropertyKeyRef(id: Int) extends TokenRef[PropertyKeyDef]

final case class RelTypeDef(handle: String, name: String) extends TokenDef
final case class RelTypeRef(id: Int) extends TokenRef[RelTypeDef]

sealed trait AnyEntity
final case class AnyNode(field: Field) extends AnyEntity
final case class AnyRelationship(field: Field) extends AnyEntity

final case class Predicate(expr: Expr)

sealed trait Expr {
  def usedFields: Set[Field] = Set.empty
  def usedLabels: Set[LabelRef] = Set.empty
  def usedRelTypes: Set[RelTypeRef] = Set.empty
  def usedPropertyKeys: Set[PropertyKeyRef] = Set.empty
}

final case class Var(name: String) extends Expr

final case class Connected(source: Field, rel: Field, target: Field) extends Expr {
  override def usedFields = Set(source, rel, target)
}

final case class HasLabel(node: Expr, label: LabelRef) extends Expr
final case class HasType(rel: Expr, relType: RelTypeRef) extends Expr
final case class Equals(lhs: Expr, rhs: Expr) extends Expr

case class Property(m: Expr, key: PropertyKeyRef) extends Expr

sealed trait Literal extends Expr
final case class IntegerLiteral(v: Long) extends Literal
final case class StringLiteral(v: String) extends Literal

sealed trait BlockType
case object OptionalBlock extends BlockType
case object StandardBlock extends BlockType
