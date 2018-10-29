package org.opencypher.okapi.ir.impl.parse.rewriter

import cats.implicits._
import org.opencypher.v9_0.ast.{Match, Where}
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.{Rewriter, bottomUp}

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
        case Or(lhs, rhs) if containsHasLabel(lhs) && containsHasLabel(rhs) => acc => acc -> None
        case Ors(children) if children.count(containsHasLabel) >= 2 => acc => acc -> None
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
