package util

import java.io.File
import java.nio.file.{Files, Path, Paths}

import org.json4s.jackson.JsonMethods.parse

import scala.io.Source
import scala.collection.JavaConverters._

object Utils {
  def jsonToMap(jsonStr: String): Map[String, String] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, String]]
  }

  def glob(path: String): List[File] = {
    val pathFile: java.io.File = new File(path)
    val dir = pathFile.getParent()
    val pattern = pathFile.getName

    if(pathFile.isFile()){
      return List[File](pathFile)
    } else if(pathFile.isDirectory()) {
      return pathFile.listFiles().toList.filter(_.isFile)
    } else if(pathFile.getName().contains("*")) {
      val paths: Iterator[Path] = Files.newDirectoryStream(Paths.get(dir), pattern).iterator().asScala

      val results = scala.collection.mutable.ListBuffer[File]()
      for (path <- paths) {
        val groundTruthFileName = Paths.get(dir, path.getFileName.toString).toAbsolutePath.toString
        results.append(new java.io.File(groundTruthFileName))
      }
      return results.toList
    } else {
      return null
    }
  }
}
