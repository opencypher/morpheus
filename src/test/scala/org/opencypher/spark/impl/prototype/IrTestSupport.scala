package org.opencypher.spark.impl.prototype

import org.opencypher.spark.StdTestSuite

import scala.collection.immutable.SortedSet

class IrTestSupport extends StdTestSuite {

  implicit def toField(s: Symbol): Field = Field(s.name)
  implicit def toVar(s: Symbol): Var = Var(s.name)

  protected class TestIR(override val root: RootBlock[Expr]) extends QueryRepresentation[Expr] {
    override def cypherQuery: String = "test"
    override def cypherVersion: String = "test"
    override def returns: SortedSet[(Field, String)] = SortedSet.empty[(Field, String)](fieldOrdering)
    override def params: Map[Param, String] = Map.empty
  }
}
