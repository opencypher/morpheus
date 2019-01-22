/**
  * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
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
package org.opencypher.parser

import org.opencypher.parser.CypherExpression.PropertyLiteral
import org.opencypher.parser.Pattern.generatedReferenceNamePrefix

import scala.util.Try

object Pattern {
  val empty: Pattern = Pattern()

  val generatedReferenceNamePrefix = "_UNNAMED#"
}

case class Pattern(
  topology: List[Connection] = Nil,
  constraints: Map[Reference, List[Constraint]] = Map.empty
) {

  assert(constraints.keysIterator.map(_.name).toSet.size == constraints.size,
    s"A pattern cannot have multiple entries with the same Reference name and different Reference types:\n$this"
  )

  def withConnection(c: Connection): Pattern = {
    copy(topology = c :: topology)
  }

  def withConstraint(v: Reference, cs: Constraint*): Pattern = {
    val currentConstraints = constraints.getOrElse(v, Nil)
    copy(constraints = constraints.updated(v, currentConstraints ++ cs))
  }

  def withNodeConstraints(cs: Constraint*): (NodeReference, Pattern) = {
    val nr = NodeReference(nextFreeReferenceName)
    nr -> withConstraint(nr, cs: _*)
  }

  def withRelationshipConstraints(cs: Constraint*): (RelationshipReference, Pattern) = {
    val rr = RelationshipReference(nextFreeReferenceName)
    rr -> withConstraint(rr, cs: _*)
  }

  def withPathConstraints(cs: Constraint*): (PathReference, Pattern) = {
    val pr = PathReference(nextFreeReferenceName)
    pr -> withConstraint(pr, cs: _*)
  }

  private def nextFreeReferenceName: String = {
    val maxUsedReferenceId = constraints
      .keysIterator
      .map(_.name)
      .filter(_.startsWith(generatedReferenceNamePrefix))
      .map(_.drop(generatedReferenceNamePrefix.length))
      .flatMap(s => Try(s.toInt).toOption)
      .max
    s"$generatedReferenceNamePrefix${maxUsedReferenceId + 1}"
  }

}

sealed trait Connection {
  def startVar: NodeReference

  def connectionVar: ConnectionReference

  def endVar: NodeReference

  def directed: Boolean
}

case class Path(
  startVar: NodeReference,
  pathVar: PathReference,
  endVar: NodeReference,
  directed: Boolean = true
) {
  def connectionVar: PathReference = pathVar
}

case class Relationship(
  startVar: NodeReference,
  relationshipVar: RelationshipReference,
  endVar: NodeReference,
  directed: Boolean = true
) {
  def connectionVar: RelationshipReference = relationshipVar
}

sealed trait Reference {
  def name: String
}
case class NodeReference(name: String) extends Reference
sealed trait ConnectionReference extends Reference
case class RelationshipReference(name: String) extends ConnectionReference
case class PathReference(name: String) extends ConnectionReference

sealed trait NodeConstraint
sealed trait ConnectionConstraint
sealed trait PathConstraint extends ConnectionConstraint
//sealed trait RelationshipConstraint extends ConnectionConstraint
sealed trait Constraint extends NodeConstraint with ConnectionConstraint

case class HasProperty(property: PropertyLiteral) extends Constraint
case class HasLabels(labels: Set[String] = Set.empty) extends NodeConstraint
case class HasType(relTypes: Set[String] = Set.empty) extends ConnectionConstraint
case class MinLength(min: Int) extends PathConstraint
case class MaxLength(max: Int) extends PathConstraint
