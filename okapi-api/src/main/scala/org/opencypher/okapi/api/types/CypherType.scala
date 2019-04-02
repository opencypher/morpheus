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
package org.opencypher.okapi.api.types

import cats.Monoid
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.impl.types.CypherTypeParser
import upickle.default.{readwriter, _}

trait CypherType {

  def isNullable: Boolean = false

  def containsNullable: Boolean = isNullable

  def asNullableAs(other: CypherType): CypherType = {
    if (!isNullable && other.isNullable) {
      nullable
    } else if (isNullable && !other.isNullable) {
      material
    } else {
      this
    }
  }

  def material: CypherType = this

  def &(other: CypherType): CypherType = meet(other)

  def meet(other: CypherType): CypherType = {
    if (this.subTypeOf(other)) this
    else if (other.subTypeOf(this)) other
    else {
      this -> other match {
        case (l: CTNode, r: CTNode) if l.graph == r.graph => CTNode(l.labels ++ r.labels, l.graph)
        case (l: CTRelationship, r: CTRelationship) if l.graph == r.graph =>
          val types = l.types.intersect(r.types)
          if (types.isEmpty) CTVoid
          else CTRelationship(types, l.graph)
        case (CTList(l), CTList(r)) => CTList(l & r)
        case (CTUnion(ls), CTUnion(rs)) => CTUnion({
          for {
            l <- ls
            r <- rs
          } yield l & r
        }.toSeq: _*)
        case (CTUnion(ls), r) => CTUnion(ls.map(_ & r).toSeq: _*)
        case (l, CTUnion(rs)) => CTUnion(rs.map(_ & l).toSeq: _*)
        case (CTMap(pl), CTMap(pr)) =>
          val intersectedProps = (pl.keys ++ pr.keys).map { k =>
            val ct = pl.get(k) -> pr.get(k) match {
              case (Some(tl), Some(tr)) => tl | tr
              case (Some(tl), None) => tl
              case (None, Some(tr)) => tr
              case (None, None) => CTVoid
            }
            k -> ct
          }.toMap
          CTMap(intersectedProps)
        case (_, _) => CTVoid
      }
    }
  }

  def intersects(other: CypherType): Boolean = meet(other) != CTVoid

  lazy val nullable: CypherType = {
    if (isNullable) this
    else CTUnion(this, CTNull)
  }

  def |(other: CypherType): CypherType = join(other)

  def join(other: CypherType): CypherType = {
    if (this.subTypeOf(other)) other
    else if (other.subTypeOf(this)) this
    else {
      this -> other match {
        case (l: CTRelationship, r: CTRelationship) if l.graph == r.graph => CTRelationship(l.types ++ r.types, l.graph)
        case (CTUnion(ls), CTUnion(rs)) => CTUnion(ls ++ rs)
        case (CTUnion(ls), r) => CTUnion(r +: ls.toSeq: _*)
        case (l, CTUnion(rs)) => CTUnion(l +: rs.toSeq: _*)
        case (l, r) => CTUnion(l, r)
      }
    }
  }

  def superTypeOf(other: CypherType): Boolean = other.subTypeOf(this)

  def subTypeOf(other: CypherType): Boolean = {
    this -> other match {
      case (CTVoid, _) => true
      case (l, r) if l == r => true
      case (_, CTAny) => true
      case (l, CTAnyMaterial) if !l.isNullable => true
      case (_: CTRelationship, CTRelationship) => true
      case (_: CTMap, CTMap) => true
      case (_: CTNode, CTNode) => true
      case (l: CTNode, r: CTNode)
        if l != CTNode && l.graph == r.graph && r.labels.subsetOf(l.labels) => true
      case (l: CTRelationship, r: CTRelationship)
        if l != CTRelationship && l.graph == r.graph && l.types.subsetOf(r.types) => true
      case (CTUnion(las), r: CTUnion) => las.forall(_.subTypeOf(r))
      case (l, CTUnion(ras)) => ras.exists(l.subTypeOf)
      case (CTList(l), CTList(r)) => l.subTypeOf(r)
      case (l@CTMap(lps), CTMap(rps)) =>
        if (l == CTMap) false
        else {
          (lps.keySet ++ rps.keySet).forall { key =>
            lps.getOrElse(key, CTNull).subTypeOf(rps.getOrElse(key, CTNull))
          }
        }
      case _ => false
    }
  }

  def couldBeSameTypeAs(other: CypherType): Boolean = {
    this.subTypeOf(other) || other.subTypeOf(this)
  }

  def name: String = getClass.getSimpleName.filter(_ != '$').drop(2).toUpperCase

  def graph: Option[QualifiedGraphName] = None

  def withGraph(qgn: QualifiedGraphName): CypherType = this

  def withoutGraph: CypherType = this

}

object CypherType {

  /**
    * Parses the name of CypherType into the actual CypherType object.
    *
    * @param name string representation of the CypherType
    * @return
    * @see {{{org.opencypher.okapi.api.types.CypherType#name}}}
    */
  def fromName(name: String): Option[CypherType] = CypherTypeParser.parseCypherType(name)

  implicit val typeRw: ReadWriter[CypherType] = readwriter[String].bimap[CypherType](_.name, s => fromName(s).get)

  implicit val joinMonoid: Monoid[CypherType] = new Monoid[CypherType] {
    override def empty: CypherType = CTVoid

    override def combine(x: CypherType, y: CypherType): CypherType = x | y
  }

}
