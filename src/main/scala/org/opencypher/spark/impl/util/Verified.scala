package org.opencypher.spark.impl.util

trait Verified[V] {
  def verified: V

  override def hashCode(): Int = verified.hashCode() + 13

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: Verified[_] => other.getClass.equals(getClass) && verified.equals(other.verified)
    case _ => false
  }
}
