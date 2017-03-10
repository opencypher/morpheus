package org.opencypher.spark.prototype.api.record

import org.opencypher.spark.api.CypherType
import org.opencypher.spark.prototype.api.expr.Expr

/*
 (1) we have to distinguish between three kinds of fields
 - fields requested by the user externally (external fields)
 - fields propagated so that the planner can execute the query (internal fields)
 - fields propagated as a more efficient way to represent the fields requested by the user (aux fields)

 (2) each df column needs a unique physical name

 (3) as a user, i want to
 - get a data frame where there is a column for each thing I put in return (RETURN n AS m, n AS k)
 - even if this implies redundancy!
 - have an easy way of finding aux columns
 - see no other (lingering) columns

 (4) as a planner, i want to
 - push around as few columns as possible (i.e. handle aliasing and not being redundant)
 - easily add columns for internal use by the planner

 model:

 exprs don't use user visible names! they use vars which are registered globally and handle aliasing

  MATCH (n) -> MATCH (#0)             | #0 -> n
  RETURN n AS m, n AS k -> RETURN #0  | #0 -> n, k

  MATCH (n)
  WITH n AS k, n AS m
  RETURN k.prop AS foo, m.prop AS bar

  n, preds(n), k = n, m = n ==> k.prop === n.prop

  MATCH (n)
  RETURN n.prop AS foo, n.prop AS bar


  MATCH (n) WITH n AS m, n AS k WITH m.prop AS foo, k.prop AS bar

  MATCH (n:Person) RETURN n AS m, n AS k | :Person(prop)
  "_0" -> Var(m), "_0" -> Var(k), "_0.prop" -> Var(m.prop), "_0.prop" -> Var(n.prop)

  n n.prop
  | |

  view.records("k")        => records
  view.records.select("k") => records
  view.records.col("k")    => string
  view.project("[n IN list | n + 5]", "newList")

  view.project(AliasedExpr)

  [X] "m" -> Var(m), "m" -> Var(k)
  [-] "a" -> Var(m), "b" -> Var(m)


  View.records           => "redundant|external"
  View.records.redundant => "..."

 only views assign field names explicitly
 there's always a mapping from variable to field name(s)
 this can be used to find aux fields (n.prop, hasLabel, labels, type...)
 field naming is uniquely based on the expr
 unless it's a var in which case it may be aliased instead

 project(expr)
 project(expr -> alias)
 select(exprs)
 select(aliases)

 records.aliases
 records.variables
 records.properties(varName)
 records.labels(varName)
 */
final case class RecordSlot(name: String, expr: Expr, cypherType: CypherType)
