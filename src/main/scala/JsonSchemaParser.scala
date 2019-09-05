import fastparse.{Parsed, _}
import MultiLineWhitespace._

import scala.io.Source
import JsonSchema._


object JsonSchemaParser {

  def stringChars(c: Char) = c != '\"' && c != '\\'

  def `null`[_: P]: P[JsonSchemaType]        = P( "null" ).map(_ => Null)
  def `false`[_: P]: P[JsonSchemaType]       = P( "false" ).map(_ => Bool)
  def `true`[_: P]: P[JsonSchemaType]        = P( "true" ).map(_ => Bool)
  def `bool`[_: P]: P[JsonSchemaType]        = P( "boolean" ).map(_ => Bool)
  def `str`[_: P]: P[JsonSchemaType]        = P( "string" ).map(_ => Str)
  def `num`[_: P]: P[JsonSchemaType]        = P( ("number" | "integer") ).map(_ => Num)
  def `arr`[_: P]: P[JsonSchemaType]        = P( "array" ).map(_ => Arr)
  def `obj`[_: P]: P[JsonSchemaType]        = P( "object" ).map(_ => Obj)
  def `empty`[_: P]: P[JsonSchemaType]        = P( "empty" ).map(_ => Empty) // for Baazizi

  def hexDigit[_: P]      = P( CharIn("0-9a-fA-F") )
  def unicodeEscape[_: P] = P( "u" ~ hexDigit ~ hexDigit ~ hexDigit ~ hexDigit )
  def escape[_: P]        = P( "\\" ~ (CharIn("\"/\\\\bfnrt") | unicodeEscape) )

  def strChars[_: P] = P( CharsWhile(stringChars) )
  def string[_: P]: P[String] = P( "\"" ~/ (strChars | escape).rep.! ~ "\"").map(_.toString)

  def types[_: P]: P[JsonSchemaType] = P( "\"" ~/ (`null` | `bool` | `false` | `true` | `str` | `num` | `arr` | `obj` | `empty`) ~/ "\"").map(_.asInstanceOf[JsonSchemaType])

  def definitions[_: P]: P[JsonSchemaStructure] = P( "\"definitions\"" ~/ ":" ~/ string ).map(JSA_definitions(_))
  def schema[_: P]: P[JsonSchemaStructure] = P( "\"$schema\"" ~/ ":" ~/ string ).map(JSA_schema(_))
  def id[_: P]: P[JsonSchemaStructure] = P( "\"$id\"" ~/ ":" ~/ string ).map(JSA_id(_))
  def ref[_: P]: P[JsonSchemaStructure] = P( "\"$ref\"" ~/ ":" ~/ string ).map(JSA_ref(_))
  def `type`[_: P]: P[JsonSchemaStructure] = P( "\"type\"" ~/ ":" ~/ types ).map(JSA_type(_))
  def title[_: P]: P[JsonSchemaStructure] = P( "\"title\"" ~/ ":" ~/ string ).map(JSA_title(_))
  def required[_: P]: P[JsonSchemaStructure] = P( "\"required\"" ~/ ":" ~/ array ).map(JSA_required(_))
  def properties[_: P]: P[JsonSchemaStructure] = P( "\"properties\"" ~/ ":" ~/ "{" ~/ jsonProperty.rep(sep=","./) ~ "}").map(JSA_properties(_))
  def description[_: P]: P[JsonSchemaStructure] = P( "\"description\"" ~/ ":" ~/ string ).map(JSA_description(_))

  def array[_: P]: P[Array[String]] = P( "[" ~/ string.rep(sep=","./) ~ "]").map(Array[String](_:_*))

  def jsonProperty[_: P]: P[JsonSchemaProperty] = P( string ~/ ":" ~/ "{" ~/ (`type` | required | properties | description).rep(sep=","./) ~ "}" ).map(x => JsonSchemaProperty(x._1,x._2))

  def jsonSchema[_: P]: P[JsonSchemaObject] = P( "{" ~/ (definitions | schema | id | `type` | title | required | properties | description).rep(sep=","./) ~ "}").map(JsonSchemaObject(_))
  def jsonExpr[_: P]: P[JsonSchemaObject] = P(jsonSchema)

  def main(args: Array[String]): Unit = {
    val s: String = Source.fromFile("sample.txt").getLines.mkString("\n")
    val Parsed.Success(value, _) = parse(s, jsonExpr(_))
    println(value)
  }

  def jsFromString(s: String): JsonSchemaObject = {
    val Parsed.Success(value, _) = parse(s, jsonExpr(_))
    return value
  }

  def jsFromFile(fileName: String): JsonSchemaObject = {
    val s: String = Source.fromFile(fileName).getLines.mkString("\n")
    return jsFromString(s)
  }

}
