package Types

import com.fasterxml.jackson.core.JsonToken._
import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

object Json {

  // custom parser, converts to our type annotation and strips values for compact form
  private def getJEObj(parser: JsonParser): JsonExplorerType = {
    // stack containing previous JsonExplorerType
    val JEStack: scala.collection.mutable.Stack[JsonExplorerType] = scala.collection.mutable.Stack[JsonExplorerType]()
    while(!parser.isClosed){
      parser.nextToken() match {
        case FIELD_NAME =>
        // basic types
        case VALUE_STRING => JEStack.top.add(parser.getCurrentName,JE_String)
        case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT => JEStack.top.add(parser.getCurrentName,JE_Numeric)
        case VALUE_NULL => JEStack.top.add(parser.getCurrentName,JE_Null)
        case VALUE_TRUE | VALUE_FALSE => JEStack.top.add(parser.getCurrentName,JE_Boolean)
        case START_OBJECT =>
          JEStack.push(new JE_Object(new scala.collection.mutable.HashMap[String,JsonExplorerType]()))

        case END_OBJECT =>
          var JEval = JEStack.pop()
          if(JEval.isEmpty())
            JEval = JE_Empty_Object
          if(JEStack.isEmpty) { // ended base object, don't want to read null after so add to
            return JEval
          } else { // pop off stack and add to new top
            JEStack.top.add(parser.getCurrentName,JEval)
          }
        case START_ARRAY =>
          JEStack.push(new JE_Array(new scala.collection.mutable.ListBuffer[JsonExplorerType]()))
        case END_ARRAY =>
          var JEval = JEStack.pop()
          if(JEval.isEmpty())
            JEval = JE_Empty_Array
          JEStack.top.add(parser.getCurrentName,JEval)

        case VALUE_EMBEDDED_OBJECT => throw new UnknownTypeException("Embedded_Object_Found??? " + parser.getCurrentToken.asString())
      }
    }
    throw new UnknownTypeException("Unescaped")
  }

  def shred(row: String): JsonExplorerType = {
    val factory = new JsonFactory()
    val parser: JsonParser = factory.createParser(row)
    return getJEObj(parser)
  }


  sealed trait JsonExplorerType {
    def add(name: String, jet: JsonExplorerType): Unit = ???
    def isEmpty(): Boolean = ???
  }

  case object JE_Null extends JsonExplorerType
  case object JE_String extends JsonExplorerType
  case object JE_Numeric extends JsonExplorerType
  case object JE_Boolean extends JsonExplorerType
  case object JE_Empty_Array extends JsonExplorerType
  case object JE_Empty_Object extends JsonExplorerType
  case object JE_Array extends JsonExplorerType
  case object JE_Object extends JsonExplorerType

  case class JE_Array(xs:ListBuffer[JsonExplorerType]) extends JsonExplorerType {
    def unapply(arg: JE_Array): Option[ListBuffer[JsonExplorerType]] = return Some(arg.asInstanceOf[ListBuffer[JsonExplorerType]])
    override def add(name: String, jet: JsonExplorerType): Unit = {xs += jet}
    override def isEmpty(): Boolean = xs.isEmpty
  }

  case class JE_Object(xs:scala.collection.mutable.HashMap[String,JsonExplorerType]) extends JsonExplorerType {
    def unapply(arg: JE_Object): Option[scala.collection.mutable.HashMap[String,JE_Object]] = return Some(arg.asInstanceOf[scala.collection.mutable.HashMap[String, JE_Object]])
    override def add(name: String, jet: JsonExplorerType): Unit = {
      xs.put(name,jet)
    }
    override def isEmpty(): Boolean = xs.isEmpty
  }
  case object JE_Unknown extends JsonExplorerType

  type AttributeName = scala.collection.mutable.ListBuffer[Any]

  final case class UnknownTypeException(private val message: String = "",
                                        private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

  case object Star {
    override def toString: String = "[*]"
  }

}
