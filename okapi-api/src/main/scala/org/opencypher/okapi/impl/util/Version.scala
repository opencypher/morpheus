package org.opencypher.okapi.impl.util

import org.opencypher.okapi.impl.exception.IllegalArgumentException

import scala.util.Try
object Version {
  def apply(versionString: String): Version = {
    versionString.split('.').map(i => Try(i.toInt).toOption).toList match {
      case Some(major) :: Some(minor) :: Nil => Version(major, minor)
      case Some(major) :: Nil => Version(major, 0)
      case _ => throw IllegalArgumentException("A version of the format major.minor", versionString, "Malformed version")
    }
  }
}

case class Version(major: Int, minor: Int) {
  override def toString: String = s"$major.$minor"

  def compatibleWith(other: Version): Boolean = other.major == major
}
