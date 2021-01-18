package Metrics

import Types.JsonSchema.{Arr, Bool, JSS, JsonSchemaType, Null, Num, Obj, Str}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Precision {

  def calculatePrecision(schema: JSS, name: ListBuffer[Any] = ListBuffer[Any](), countedAdditionalProperties: mutable.Set[ListBuffer[Any]] = mutable.Set[ListBuffer[Any]]()): BigInt = {
    val requiredSet: Set[String] = schema.required match {
      case Some(v) => v.value
      case None => Set[String]()
    }

    schema.anyOf match {
      case None =>
        schema.oneOf match {
          case Some(oneOf) => oneOf.value.map(calculatePrecision(_, name, countedAdditionalProperties)).reduce(_+_)
          case None =>
            schema.items match {
              case None =>
                val additonalProperties = schema.additionalProperties match {
                  case Some(p) =>
                    p.value
                  case None => false
                }
                if(additonalProperties && countedAdditionalProperties.contains(name)) {
                  BigInt(1)
                } else {
                  if(additonalProperties) countedAdditionalProperties.add(name)
                  schema.properties match {
                    case None => return BigInt(1) // leaf
                    case Some(leaf) =>
                      //(schema.`type`.equals(Arr) && schema.maxItems.getOrElse(1.0) == 0.0) || (schema.`type`.equals(Obj) && schema.maxProperties.getOrElse(1.0) == 0.0)
                      if (leaf.value.size == 0) { // is an empty array or empty obj
                        BigInt(1)
                      } else {
                        leaf.value.map(x => {
                          if (!x._2.oneOf.equals(None)) // is a oneOf
                            calculatePrecision(x._2, name ++ ListBuffer[Any](x._1), countedAdditionalProperties) + (if (requiredSet.contains(x._1)) BigInt(0) else BigInt(1))
                          else
                            calculatePrecision(x._2, name ++ ListBuffer[Any](x._1), countedAdditionalProperties) * (if (requiredSet.contains(x._1)) BigInt(1) else BigInt(2))
                        }).reduce(_*_)
                      }
                  }
                }

              case Some(item) => calculatePrecision(item.value, name, countedAdditionalProperties)
            }
        }
      case Some(anyOf) =>
        val v = anyOf.value.map(calculatePrecision(_, name, countedAdditionalProperties))
        v.reduce(_+_)
    }
  }

}
