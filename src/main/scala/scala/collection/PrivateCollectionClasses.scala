package scala.collection

import scala.collection.immutable.RedBlackTree.{BlackTree, RedTree, Tree}

object PrivateCollectionClasses {

  val registeredClasses = Seq(classOf[BlackTree[String, AnyRef]],
    classOf[RedTree[String, AnyRef]],
    classOf[Tree[String, AnyRef]])

}
