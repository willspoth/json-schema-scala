package Metrics

import Types.Json.{JE_Array, JE_Boolean, JE_Empty_Array, JE_Empty_Object, JE_Null, JE_Numeric, JE_Object, JE_String}
import Types.JsonSchema.{Arr, Bool, JSS, Null, Num, Obj, Str}
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory


object Validation {

  val logger = Logger(LoggerFactory.getLogger("Validation"))

  def calculateValidation(schema: JSS, rows: Array[String]): Double = {
    rows.map(x => if(validateRow(schema,Types.Json.shred(x))) 1.0 else 0.0).reduce(_+_) / rows.size.toDouble
  }

  def validateRow(schema: JSS, attribute: Types.Json.JsonExplorerType, name: String = ""): Boolean = {
    logger.trace(name + ": validating attribute")

    schema.anyOf match {
      case Some(anyOf) =>
        anyOf.value.map( s => validateRow(s,attribute,name))
          .reduce(_||_)
      case None =>
        attribute match {
          case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object =>
            val typeCheck = compareTypes(schema,attribute)
            if (typeCheck)
              logger.debug(name + ": " + attribute.getClass + " found and ok")
            else
              logger.debug(name + ": " + attribute.getClass + " found : " + schema.`type`.getOrElse(None).toString)
            typeCheck

          case JE_Object(m) =>
            schema.maxProperties match { // check on constraint
              case None =>
              case Some(max) =>
                if (m.size > max.value) {
                  logger.debug(name + " maxProperties: " + max.value.toString + " found size: " + m.size.toString)
                  return false
                }
            }

            val passesRequiredCheck: Boolean = schema.required match {
              case Some(req) =>
                if ((req.value.size > 0)) req.value.map(m.contains(_)).reduce(_&&_)
                else true
              case None => true
            }

            if(passesRequiredCheck == false) logger.debug(name+": Failed required check")

            val allAttributesPass: Boolean = m.map{ case(n,t) => {
              schema.properties match {
                case Some(s) =>
                  s.value.get(n) match {
                    case Some(v) =>
                      validateRow(v, t, n)
                    case None =>
                      logger.debug(name+": Attribute not found")
                      return false
                  }
                case None =>
                  logger.debug(name+": No properties Found")
                  return false
              }
            }}.reduce(_&&_)

            schema.`type`.get.value.equals(Obj) &&
            passesRequiredCheck &&
            allAttributesPass


          case JE_Array(a) =>
            schema.maxItems match { // check on constraint
              case None =>
              case Some(max) =>
                if (a.size > max.value){
                  logger.debug(name + " maxItems: " + max.value.toString + " found size: " + a.size.toString)
                  return false
                }
            }

            if(!schema.required.equals(None)){
              logger.error(name+": Unexpected required fields in array")
            }

            val arrayCheck: Boolean = (a.map(sArr => {
              schema.items match {
                case Some(s) =>
                  validateRow(s.value,sArr, name+"[*]")
                case None =>
                  logger.debug(name+": Empty Array Found")
                  return false
              }
            }).reduce(_&&_))

            schema.`type`.get.value.equals(Arr) &&
            arrayCheck

        }
    }

  }

  private def compareTypes(schema: JSS, attribute: Types.Json.JsonExplorerType): Boolean = {
    schema.`type` match {
      case Some(t) =>
        attribute match {
          case JE_String => return t.value.equals(Str)
          case JE_Numeric => return t.value.equals(Num)
          case JE_Boolean => return t.value.equals(Bool)
          case JE_Null => return t.value.equals(Null)
          case JE_Empty_Array => return t.value.equals(Arr)
          case JE_Empty_Object => return t.value.equals(Obj)
        }
      case None => // probably anyOf
        schema.anyOf match {
          case None => throw new Exception("anyOf and type not found so something's probably not right") // idk what's going on, throw an error or something
          case Some(anyOf) => return anyOf.value.map(s => compareTypes(s,attribute)).reduce(_||_)
        }
    }

  }

}
