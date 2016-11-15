package org.opencypher.spark.impl

sealed trait SupportedQuery {
  def colonList(tokens: IndexedSeq[String]) = s"${tokens.mkString(":", ":", "")}"
  def cypher: String

  override def toString: String = cypher
}

object BoundVariableLength {
  def apply(lowerBound: Int, upperBound: Int, startLabels: String*): BoundVariableLength =
    new BoundVariableLength(startLabels.toIndexedSeq, lowerBound, upperBound)
}

case class BoundVariableLength(startLabels: IndexedSeq[String] = IndexedSeq.empty, lowerBound: Int, upperBound: Int) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(startLabels)
    val f = lowerBound
    val t = upperBound

    s"MATCH (a$l)-[r*$f..$t]->() RETURN r"
  }
}

case class NodeScan(labels: IndexedSeq[String] = IndexedSeq.empty) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(labels)

    s"MATCH (n$l) RETURN (n)"
  }
}

case class NodeScanWithProjection(labels: IndexedSeq[String] = IndexedSeq.empty,
                                  firstKey: Symbol = 'name, secondKey: Symbol = 'age) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(labels)
    val n1 = firstKey.name
    val n2 = secondKey.name

    s"MATCH (n$l) RETURN n.$n1, n.$n2"
  }
}

case class SimplePattern(startLabels: IndexedSeq[String] = IndexedSeq.empty, types: IndexedSeq[String] = IndexedSeq.empty,
                         endLabels: IndexedSeq[String] = IndexedSeq.empty) extends SupportedQuery {
  override def cypher: String = {
    val s = colonList(startLabels)
    val t = colonList(types)
    val e = colonList(endLabels)

    s"MATCH ($s)-[r$t]->($e) RETURN r"
  }
}

case class SimpleUnionAll(lhsLabels: IndexedSeq[String] = IndexedSeq.empty, lhsKey: Symbol,
                          rhsLabels: IndexedSeq[String] = IndexedSeq.empty, rhsKey: Symbol) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(lhsLabels)
    val lk = lhsKey.name
    val r = colonList(rhsLabels)
    val rk = rhsKey.name

    s"MATCH (a$l) RETURN a.$lk AS name UNION ALL MATCH (b$r) RETURN b.$rk AS name"
  }
}

case class NodeScanIdsSorted(labels: IndexedSeq[String] = IndexedSeq.empty) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(labels)

    s"MATCH (n$l) RETURN id(n) AS id ORDER BY id DESC"
  }
}

case class CollectNodeProperties(labels: IndexedSeq[String] = IndexedSeq.empty, key: Symbol = 'name) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(labels)
    val k = key.name

    s"MATCH (a$l) RETURN collect(a.$k) AS names"
  }
}

case class CollectAndUnwindNodeProperties(labels: IndexedSeq[String] = IndexedSeq.empty, key: Symbol = 'name, column: Symbol) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(labels)
    val k = key.name
    val c = column.name

    s"MATCH (a$l) WITH collect(a.$k) AS names UNWIND names AS $c RETURN $c"
  }
}

case class MatchOptionalExpand(startLabels: IndexedSeq[String] = IndexedSeq.empty, types: IndexedSeq[String] = IndexedSeq.empty,
                               endLabels: IndexedSeq[String] = IndexedSeq.empty) extends SupportedQuery {
  override def cypher: String = {
    val s = colonList(startLabels)
    val t = colonList(types)
    val e = colonList(endLabels)

    s"MATCH (a$s) OPTIONAL MATCH (a)-[r$t]->(b$e) RETURN r"
  }
}
