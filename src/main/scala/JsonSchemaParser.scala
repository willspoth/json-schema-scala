import fastparse._
import MultiLineWhitespace._
import scala.io.Source
import JsonSchemaTypes._


object JsonSchemaParser {

  def stringChars(c: Char) = c != '\"' && c != '\\'

  def space[_: P]         = P( CharsWhileIn(" \r\n", 0) )
  def digits[_: P]        = P( CharsWhileIn("0-9") )
  def exponent[_: P]      = P( CharIn("eE") ~ CharIn("+\\-").? ~ digits )
  def fractional[_: P]    = P( "." ~ digits )
  def integral[_: P]      = P( "0" | CharIn("1-9")  ~ digits.? )

  def number[_: P] = P(  CharIn("+\\-").? ~ integral ~ fractional.? ~ exponent.? ).!

  def `null`[_: P]        = P( "null" )
  def `false`[_: P]       = P( "false" )
  def `true`[_: P]        = P( "true" )

  def hexDigit[_: P]      = P( CharIn("0-9a-fA-F") )
  def unicodeEscape[_: P] = P( "u" ~ hexDigit ~ hexDigit ~ hexDigit ~ hexDigit )
  def escape[_: P]        = P( "\\" ~ (CharIn("\"/\\\\bfnrt") | unicodeEscape) )

  def strChars[_: P] = P( CharsWhile(stringChars) )
  def string[_: P]: P[String] = P( "\"" ~/ (strChars | escape).rep.! ~ "\"").map(_.toString)

  //def pair[_: P]: P[JSA_pair] = P( string ~/ ":" ~/ obj ).map(v => JSA_pair(v._1,v._2))

  def definitions[_: P]: P[JsonSchemaStructure] = P( "\"definitions\"" ~/ ":" ~/ string ).map(JSA_definitions(_))
  def schema[_: P]: P[JsonSchemaStructure] = P( "\"$schema\"" ~/ ":" ~/ string ).map(JSA_schema(_))
  def id[_: P]: P[JsonSchemaStructure] = P( "\"$id\"" ~/ ":" ~/ string ).map(JSA_id(_))
  def `type`[_: P]: P[JsonSchemaStructure] = P( "\"type\"" ~/ ":" ~/ string ).map(JSA_type(_))
  def title[_: P]: P[JsonSchemaStructure] = P( "\"title\"" ~/ ":" ~/ string ).map(JSA_title(_))
  def required[_: P]: P[JsonSchemaStructure] = P( "\"required\"" ~/ ":" ~/ array ).map(JSA_required(_))
  def properties[_: P]: P[JsonSchemaStructure] = P( "\"properties\"" ~/ ":" ~/ "{" ~/ jsonProperty.rep(sep=","./) ~ "}").map(JSA_properties(_))
  def description[_: P]: P[JsonSchemaStructure] = P( "\"description\"" ~/ ":" ~/ string ).map(JSA_description(_))

  def array[_: P]: P[Array[String]] = P( "[" ~/ string.rep(sep=","./) ~ "]").map(Array[String](_:_*))

  def jsonProperty[_: P]: P[JsonSchemaProperty] = P( string ~/ ":" ~/ "{" ~/ (`type` | required | properties | description).rep(sep=","./) ~ "}" ).map(x => JsonSchemaProperty(x._1,x._2))

  def jsonSchema[_: P]: P[JsonSchema] = P( "{" ~/ (definitions | schema | id | `type` | title | required | properties | description).rep(sep=","./) ~ "}").map(JsonSchema(_))
  def jsonExpr[_: P]: P[JsonSchema] = P(jsonSchema)

  def main(args: Array[String]): Unit = {
    val s: String = Source.fromFile("sample.txt").getLines.mkString("\n")
    val Parsed.Success(value, _) = parse(s, jsonExpr(_))
    println(value)
  }

}
