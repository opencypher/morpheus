import java.io.FileNotFoundException

import ammonite.ops._

import scala.io.Codec

object License {

  val successMsg = "All licenses are correct."

  val licensePath: Path = pwd / "license-header.txt"

  val expectedLicense: IndexedSeq[String] = {
    if (exists(licensePath)) {
      read.lines ! licensePath
    } else {
      throw new FileNotFoundException(licensePath.toString)
    }
  }

  val numLicenseLines = expectedLicense.length

  def pathAsListItem(path: Path): String = {
    s" - ${path.segments.last}"
  }

  def pathsAsListItems(paths: Traversable[Path]): String = {
    paths.map(pathAsListItem).mkString("", "\n", "\n")
  }

  def check(path: Path, codec: Codec = Codec.UTF8): Boolean = {
    (read.lines ! path).startsWith(expectedLicense)
  }

  def add(path: Path, codec: Codec = Codec.UTF8): Boolean = {
    val fileContent = read.lines ! path
    if (!fileContent.startsWith(expectedLicense)) {
      val sb = StringBuilder.newBuilder
      sb.append(expectedLicense.mkString("", "\n", "\n"))
      sb.append(fileContent.mkString("", "\n", "\n"))
      write.over(path, sb.toString())
      true
    } else {
      false
    }
  }

}
