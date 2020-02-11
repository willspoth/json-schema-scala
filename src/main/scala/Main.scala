import Types.JsonSchema.JSS
import org.apache.log4j.{BasicConfigurator, Level, Logger}

import scala.io.Source

object Main {

  def main(args: Array[String]): Unit = {
    val config = readArgs(args)

    BasicConfigurator.configure()
    val validationLogger: Logger  = Logger.getLogger(Metrics.Validation.getClass)
    validationLogger.setLevel(config.logLevel)

    val schema: JSS = JsonSchemaParser.jsFromFile(config.schemaFile)

    if(config.calculatePrecision)
      println("Precision: "+ Metrics.Precision.calculatePrecision(schema).toString())

    println("Grouping: "+ Metrics.Grouping.calculateGrouping(schema).toString())

    config.validate match {
      case Some(s) =>
        if (s.charAt(0).equals('{')) { // guess is string for now
          val v = Metrics.Validation.calculateValidation(schema,Array(s))
          if((v._1/v._3) == 1.0) println("true") else println("false")
        } else {
          val f = new java.io.File(s)
          if(f.exists() && f.isFile){ // is single file
            val v = Metrics.Validation.calculateValidation(schema,Source.fromFile(s).getLines.toArray)
            println("Validation: " + (v._1/v._3).toString)
            println("Schema Saturation: " + v._2.mkString(","))
          } else if(f.exists() && f.isDirectory){ // is directory
            val files = getListOfFiles(s)
            val totalVal = files.filter(x => !x.getName.charAt(0).equals('_') && !x.getName.charAt(0).equals('.')).map(file => {
              val v = Metrics.Validation.calculateValidation(schema,Source.fromFile(file.toString).getLines.toArray)
              println(file.getName+" validation: " + (v._1/v._3).toString)
              println("Schema Saturation: " + v._2.mkString(","))
              v
            }).map(_._1).reduce(_*_)
            println("Dir " + s + " Validation: " + totalVal.toString)

          } else {
            throw new Exception("file " + s + " existence: " + f.exists().toString)
          }
        }
      case None => // do nothing
    }

  }

  def readArgs(args: Array[String]): config = {
    if (args.size == 0 || args.size % 2 == 0) {
      println("Unexpected Argument, should be, schemaFileName val string|file|dir prec true logLevel INFO")
      System.exit(0)
    }
    val argMap = scala.collection.mutable.HashMap[String, String]()
    val schemaFile: String = args(0)
    if (args.tail.size > 1) {
      args.tail.zip(args.tail.tail).zipWithIndex.filter(_._2 % 2 == 0).map(_._1).foreach(x => argMap.put(x._1, x._2))
    }

    val calculatePrecision: Boolean = argMap.get("prec") match {
      case Some("true" | "t" | "y" | "yes") => true
      case Some("n" | "no" | "false" | "f") => false
      case _ | None => true
    }

    val logLevel: Level = argMap.get("logLevel") match {
      case Some(s) => s.toUpperCase() match {
        case "TRACE" => Level.TRACE
        case "DEBUG" => Level.DEBUG
        case "ALL" => Level.ALL
        case "INFO" => Level.INFO
        case "WARN" => Level.WARN
        case _ => Level.WARN
      }
      case _ | None => Level.OFF
    }

    val validate: Option[String] = argMap.get("val")

    config(
      schemaFile,
      calculatePrecision,
      validate,
      logLevel
    )
  }

  case class config(schemaFile: String,
                    calculatePrecision: Boolean,
                    validate: Option[String],
                    logLevel: Level
                   )

  def getListOfFiles(dir: String):List[java.io.File] = {
    val d = new java.io.File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[java.io.File]()
    }
  }

}
