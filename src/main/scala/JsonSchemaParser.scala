import fastparse._
import MultiLineWhitespace._
import scala.io.Source


sealed trait JsonSchemaTypes

case object JSStr extends JsonSchemaTypes
case object JSObj extends JsonSchemaTypes
case object JSArr extends JsonSchemaTypes
case object JSNum extends JsonSchemaTypes
case object JSBool extends JsonSchemaTypes
case object JSRef extends JsonSchemaTypes


sealed trait JsonSchemaStructure extends Any

case class JSA_definitions(value: java.lang.String) extends JsonSchemaStructure
case class JSA_schema(value: java.lang.String) extends JsonSchemaStructure
case class JSA_id(value: java.lang.String) extends JsonSchemaStructure
case class JSA_type(value: java.lang.String) extends JsonSchemaStructure
case class JSA_title(value: java.lang.String) extends JsonSchemaStructure
case class JSA_required(value: Array[String]) extends JsonSchemaStructure
case class JSA_properties(value: JsonSchema) extends JsonSchemaStructure
case class JSA_pair(key: java.lang.String, value: java.lang.String) extends JsonSchemaStructure


case class JsonSchema(value: Seq[JsonSchemaStructure])//value: scala.collection.mutable.HashMap[String, JsonSchemaStructure])


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

  def pair[_: P]: P[JSA_pair] = P( string ~/ ":" ~/ string ).map(v => JSA_pair(v._1,v._2))

  def definitions[_: P]: P[JsonSchemaStructure] = P( "\"definitions\"" ~/ ":" ~/ string ).map(JSA_definitions(_))
  def schema[_: P]: P[JsonSchemaStructure] = P( "\"$schema\"" ~/ ":" ~/ string ).map(JSA_schema(_))
  def id[_: P]: P[JsonSchemaStructure] = P( "\"$id\"" ~/ ":" ~/ string ).map(JSA_id(_))
  def `type`[_: P]: P[JsonSchemaStructure] = P( "\"type\"" ~/ ":" ~/ string ).map(JSA_type(_))
  def title[_: P]: P[JsonSchemaStructure] = P( "\"title\"" ~/ ":" ~/ string ).map(JSA_title(_))
  def required[_: P]: P[JsonSchemaStructure] = P( "\"required\"" ~/ ":" ~/ array ).map(JSA_required(_))
  def properties[_: P]: P[JsonSchemaStructure] = P( "\"properties\"" ~/ ":" ~/ obj ).map(JSA_properties(_))

  def array[_: P]: P[Array[String]] = P( "[" ~/ string.rep(sep=","./) ~ "]").map(Array[String](_:_*))

  def obj[_: P]: P[JsonSchema] = P( "{" ~/ (definitions | schema | id | `type` | title | required | properties | pair).rep(sep=","./) ~ "}").map(JsonSchema(_))
  /*_.foldLeft(scala.collection.mutable.HashMap[String,JsonSchemaStructure]()){case(acc,v)=>{
    v match {
      case _:JSA_definitions => acc.put("",v)
    }
    acc
  }
  })
*/
  def jsonExpr[_: P]: P[JsonSchema] = P(obj)

  def main(args: Array[String]): Unit = {
    val s: String = Source.fromFile("sample.txt").getLines.mkString("\n")
    val Parsed.Success(value, _) = parse(s, jsonExpr(_))
    println(value)
  }

}
