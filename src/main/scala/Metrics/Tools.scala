package Metrics

import Metrics.Count.countAttributes
import Types.JsonSchema.JSS

import scala.collection.mutable

object Tools {

  def diff(l: JSS, r: JSS) = {
    val lAtt = getAttributes(l).map(_.toString())
    val rAtt = getAttributes(r).map(_.toString())
    println(lAtt.diff(rAtt))
    println("diff done")
  }


  case object Star {
    override def toString: String = "[*]"
  }

  def getAttributes(schema: JSS,name: mutable.ListBuffer[Any] = mutable.ListBuffer[Any](), attributes: mutable.Set[mutable.ListBuffer[Any]] = mutable.Set[mutable.ListBuffer[Any]]()): mutable.Set[mutable.ListBuffer[Any]] = {

    schema.anyOf match {
      case None =>
        schema.items match {
          case None =>
            schema.properties match {
              case None => attributes += name // leaf
              case Some(leaf) =>
                if(leaf.value.size == 0) // is an empty obj
                  attributes += name
                leaf.value.map(x => {
                  getAttributes(x._2,name++List(x._1),attributes)
                })
            }
          case Some(item) => getAttributes(item.value,name++List(Star),attributes)
        }
      case Some(anyOf) =>
        val t = anyOf.value.map(countAttributes(_))
        anyOf.value.map(x => getAttributes(x,name,attributes))
    }

    if (name.isEmpty)
      return attributes
    else
      return null
  }

}
