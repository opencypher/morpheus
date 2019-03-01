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
package org.opencypher.okapi.ir.impl.parse.rewriter

import cats.implicits._
import org.neo4j.cypher.internal.v4_0.ast.{Match, Where}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, bottomUp}

case object pushLabelsIntoScans extends Rewriter {
  override def apply(that: AnyRef): AnyRef = {
    instance(that)
  }

  private val rewriter = Rewriter.lift {
    case m: Match =>
      def containsHasLabel(e: Expression) = e.treeExists {
        case _:HasLabels => true
      }

      val patternLabelMap = m.pattern.treeFold[Map[String, Set[LabelName]]](Map.empty) {
        case NodePattern(Some(Variable(name)), labels, _, _) =>
          acc => {
            val updatedLabels = acc.getOrElse(name, Set.empty) ++ labels
            val updated = acc.updated(name, updatedLabels)
            val merge = (other: Map[String, Set[LabelName]]) => updated |+| other
            updated -> Some(merge)
          }
      }

      val whereLabelMap = m.where.treeFold[Map[String, Set[LabelName]]](Map.empty) {
        case Or(lhs, rhs) if containsHasLabel(lhs) || containsHasLabel(rhs) => acc => acc -> None
        case Ors(children) if children.count(containsHasLabel) >= 1 => acc => acc -> None
        case Not(child) if containsHasLabel(child) => acc => acc -> None
        case Xor(lhs, rhs) if containsHasLabel(lhs) || containsHasLabel(rhs) => acc => acc -> None
        case HasLabels(Variable(name), labels) =>
          acc => {
            val updatedLabels = acc.getOrElse(name, Set.empty) ++ labels
            val updated = acc.updated(name, updatedLabels)
            val merge = (other: Map[String, Set[LabelName]]) => updated |+| other
            updated -> Some(merge)
          }
      }

      val labelMap = patternLabelMap |+| whereLabelMap
      val pattern = m.pattern.endoRewrite(addLabelsToNodePatterns(labelMap))
      val where = m.where.endoRewrite(removeRedundantLabelFilters(labelMap)) match {
        case Some(Where(_: True)) => None
        case other => other
      }
      m.copy(pattern = pattern, where = where)(m.position)
  }

  private def addLabelsToNodePatterns(labelMap: Map[String, Set[LabelName]]) = bottomUp {
    case n @ NodePattern(Some(v), _, properties, base) if labelMap.contains(v.name) =>
      NodePattern(Some(v), labelMap(v.name).toSeq, properties, base)(n.position)
    case other => other
  }

  private def removeRedundantLabelFilters(labelMap: Map[String, Set[LabelName]]) = bottomUp {
    case h@HasLabels(Variable(name), labels) if labels.toSet subsetOf labelMap.getOrElse(name, Set.empty) => True()(h.position)
    case a@And(_: True, _: True) => True()(a.position)
    case a@And(_: True, other) => other
    case a@And(other, _: True) => other
    case a@Ands(exprs) =>
      val keep = exprs.filter {
        case _:True => false
        case _ => true
      }
      if (keep.isEmpty) True()(a.position) else Ands(keep)(a.position)
    case other => other
  }

  private val instance: Rewriter = bottomUp(rewriter, _.isInstanceOf[Expression])
}
