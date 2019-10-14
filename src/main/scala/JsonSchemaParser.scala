import fastparse.{Parsed, _}
import MultiLineWhitespace._

import scala.io.Source
import Types.JsonSchema._
import org.apache.log4j.{BasicConfigurator, Level, Logger}


object JsonSchemaParser {

  def stringChars(c: Char) = c != '\"' && c != '\\'

  def `trueB`[_: P]: P[Boolean]        = P( "true" ).map(_ => true)
  def `falseB`[_: P]: P[Boolean]        = P( "false" ).map(_ => false)

  def `null`[_: P]: P[JsonSchemaType]        = P( "null" ).map(_ => Null)
  def `false`[_: P]: P[JsonSchemaType]       = P( "false" ).map(_ => Bool)
  def `true`[_: P]: P[JsonSchemaType]        = P( "true" ).map(_ => Bool)
  def `bool`[_: P]: P[JsonSchemaType]        = P( "boolean" ).map(_ => Bool)
  def `str`[_: P]: P[JsonSchemaType]        = P( "string" ).map(_ => Str)
  def `num`[_: P]: P[JsonSchemaType]        = P( ("number" | "integer") ).map(_ => Num)
  def `arr`[_: P]: P[JsonSchemaType]        = P( "array" ).map(_ => Arr)
  def `obj`[_: P]: P[JsonSchemaType]        = P( "object" ).map(_ => Obj)

  def hexDigit[_: P]      = P( CharIn("0-9a-fA-F") )
  def unicodeEscape[_: P] = P( "u" ~ hexDigit ~ hexDigit ~ hexDigit ~ hexDigit )
  def escape[_: P]        = P( "\\" ~ (CharIn("\"/\\\\bfnrt") | unicodeEscape) )

  def strChars[_: P] = P( CharsWhile(stringChars) )
  def string[_: P]: P[String] = P( "\"" ~/ (strChars | escape).rep.! ~ "\"").map(_.toString)

  def digits[_: P]        = P( CharsWhileIn("0-9") )
  def exponent[_: P]      = P( CharIn("eE") ~ CharIn("+\\-").? ~ digits )
  def fractional[_: P]    = P( "." ~ digits )
  def integral[_: P]      = P( "0" | CharIn("1-9")  ~ digits.? )

  def number[_: P] = P(  CharIn("+\\-").? ~ integral ~ fractional.? ~ exponent.? ).!.map(
    x => x.toDouble
  )

  def types[_: P]: P[JsonSchemaType] = P( "\"" ~/ (`null` | `bool` | `false` | `true` | `str` | `num` | `arr` | `obj`) ~/ "\"").map(_.asInstanceOf[JsonSchemaType])

  //def definitions[_: P]: P[JsonSchemaStructure] = P( "\"definitions\"" ~/ ":" ~/ string ).map(JSA_definitions(_))
  def schema[_: P]: P[JsonSchemaStructure] = P( "\"$schema\"" ~/ ":" ~/ string ).map(JSA_schema(_))
  def id[_: P]: P[JsonSchemaStructure] = P( "\"$id\"" ~/ ":" ~/ string ).map(JSA_id(_))
  def ref[_: P]: P[JsonSchemaStructure] = P( "\"$ref\"" ~/ ":" ~/ string ).map(JSA_ref(_))
  def `type`[_: P]: P[JsonSchemaStructure] = P( "\"type\"" ~/ ":" ~/ types ).map(JSA_type(_))
  def title[_: P]: P[JsonSchemaStructure] = P( "\"title\"" ~/ ":" ~/ string ).map(JSA_title(_))
  def required[_: P]: P[JsonSchemaStructure] = P( "\"required\"" ~/ ":" ~/ array ).map(x => JSA_required(x.toSet))
  def properties[_: P]: P[JsonSchemaStructure] = P( "\"properties\"" ~/ ":" ~/ "{" ~/ jsonProperty.rep(sep=","./) ~ "}").map(x => JSA_properties(x.toMap))
  def description[_: P]: P[JsonSchemaStructure] = P( "\"description\"" ~/ ":" ~/ string ).map(JSA_description(_))
  def anyOf[_: P]: P[JsonSchemaStructure] = P( "\"anyOf\"" ~/ ":" ~/ "[" ~/ jsonSchema.rep(sep=","./) ~ "]").map(x => JSA_anyOf(x))
  def oneOf[_: P]: P[JsonSchemaStructure] = P( "\"oneOf\"" ~/ ":" ~/ "[" ~/ jsonSchema.rep(sep=","./) ~ "]").map(x => JSA_oneOf(x))
  def items[_: P]: P[JsonSchemaStructure] = P( "\"items\"" ~/ ":" ~/ jsonSchema ).map(x => JSA_items(x))
  def maxItems[_: P]: P[JsonSchemaStructure] = P("\"maxItems\"" ~/ ":" ~/ number ).map(x => JSA_maxItems(x))
  def maxProperties[_: P]: P[JsonSchemaStructure] = P("\"maxProperties\"" ~/ ":" ~/ number ).map(x => JSA_maxItems(x))
  def additionalProperties[_: P]: P[JsonSchemaStructure] = P("\"additionalProperties\"" ~/ ":" ~/ (`trueB` | `falseB`) ).map(x => JSA_additionalProperties(x))


  def array[_: P]: P[Array[String]] = P( "[" ~/ string.rep(sep=","./) ~ "]").map(Array[String](_:_*))

  def jsonProperty[_: P]: P[(String,JSS)] = P( string ~/ ":" ~/ jsonSchema ).map(x => (x._1,x._2))

  def jsonSchema[_: P]: P[JSS] = P( "{" ~/ ( schema | id | `type` | title | required | properties | description | anyOf | oneOf | items | maxItems | maxProperties | additionalProperties).rep(sep=","./) ~ "}").map(JSS(_))
  def jsonExpr[_: P]: P[JSS] = P(jsonSchema)


  def jsFromString(s: String): JSS = {
    val Parsed.Success(value, _) = parse(s, jsonExpr(_))
    return value
  }

  def jsFromFile(fileName: String): JSS = {
    val s: String = Source.fromFile(fileName).getLines.mkString("\n")
    return jsFromString(s)
  }

  def main(args: Array[String]): Unit = {
    val config = readArgs(args)

    BasicConfigurator.configure()
    val validationLogger: Logger  = Logger.getLogger(Metrics.Validation.getClass)
    validationLogger.setLevel(config.logLevel)

    val schema: JSS = jsFromFile(config.schemaFile)

    if(config.calculatePrecision)
      println("Precision: "+ Metrics.Precision.calculatePrecision(schema).toString())

    println("Grouping: "+ Metrics.Grouping.calculateGrouping(schema).toString())

    config.validate match {
      case Some(s) =>
        if (s.charAt(0).equals('{')) { // guess is string for now
          val v = Metrics.Validation.calculateValidation(schema,Array(s))
          if(v == 1.0) println("true") else println("false")
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
      println("Unexpected Argument, should be, schemaFileName -val string|file|dir -prec true -logLevel INFO")
      System.exit(0)
    }
    val argMap = scala.collection.mutable.HashMap[String, String]()
    val schemaFile: String = args(0)
    if (args.tail.size > 1) {
      val argPairs = args.tail.zip(args.tail.tail).zipWithIndex.filter(_._2 % 2 == 0).map(_._1).foreach(x => argMap.put(x._1, x._2))
    }

    val calculatePrecision: Boolean = argMap.get("-prec") match {
      case Some("true" | "t" | "y" | "yes") => true
      case Some("n" | "no" | "false" | "f") => false
      case _ | None => true
    }

    val logLevel: Level = argMap.get("-logLevel") match {
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

    val validate: Option[String] = argMap.get("-val")

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
