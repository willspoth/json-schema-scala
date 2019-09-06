object JsonSchema {

  sealed trait JsonSchemaType

  case object Str extends JsonSchemaType {
    override def toString: String = "string"
  }
  case object Obj extends JsonSchemaType {
    override def toString: String = "object"
  }
  case object Arr extends JsonSchemaType {
    override def toString: String = "array"
  }
  case object Num extends JsonSchemaType {
    override def toString: String = "number"
  }
  case object Bool extends JsonSchemaType {
    override def toString: String = "boolean"
  }
  case object Null extends JsonSchemaType {
    override def toString: String = "null"
  }
  case object Empty extends JsonSchemaType {
    override def toString: String = "empty"
  }


  sealed trait JsonSchemaStructure extends Any

  case class JSA_schema(value: String) extends JsonSchemaStructure {
    override def toString: String = s""""$$schema":${value.toString}"""
  }
  case class JSA_id(value: String) extends JsonSchemaStructure {
    override def toString: String = s""""$$id":${value.toString}"""
  }
  case class JSA_ref(value: String) extends JsonSchemaStructure {
    override def toString: String = s""""$$ref":${value.toString}"""
  }
  case class JSA_title(value: String) extends JsonSchemaStructure {
    override def toString: String = s""""title":${value.toString}"""
  }
  case class JSA_description(value: String) extends JsonSchemaStructure {
    override def toString: String = s""""description":${value.toString}"""
  }
  case class JSA_type(value: JsonSchemaType) extends JsonSchemaStructure {
    override def toString: String = s""""type":"${value.toString}""""
  }
  case class JSA_properties(value: Map[String,JSS]) extends JsonSchemaStructure {
    override def toString: String = s""""properties":{${value.map{case(k,v) => "\""+k+"\":"+v.toString}.mkString(",")}}"""
  }
  case class JSA_required(value: Seq[String]) extends JsonSchemaStructure {
    override def toString: String = s""""required":[${value.map(x => '"' + x + '"').mkString(",")}]"""
  }
  case class JSA_items(value: JSS) extends JsonSchemaStructure {
    override def toString: String = s""""items":${value.toString}"""
  }
  case class JSA_anyOf(value: Seq[JSS]) extends JsonSchemaStructure {
    override def toString: String = s""""anyOf":[${value.map(x => x.toString).mkString(",")}]"""
  }
  //  case class JSA_definitions(value: Map[String,JSS]) extends JsonSchemaStructure {
  //    override def toString: String = s"\"definitions\":{${value.map{case(k,v) => "\""+k+"\":"+v.toString}.mkString(",")}}"
  //  }


  case class JSS(
                  schema: Option[ JSA_schema ] = None,
                  id: Option[ JSA_id ] = None,
                  ref: Option[ JSA_ref ] = None,
                  title: Option[ JSA_title ] = None,
                  description: Option[ JSA_description ] = None,
                  `type`: Option[ JSA_type ] = None,
                  properties: Option[ JSA_properties ] = None,
                  required: Option[ JSA_required ] = None,
                  items: Option[ JSA_items ] = None,
                  anyOf: Option[ JSA_anyOf ] = None
                )
  {
    override def toString: String = {
      "{" +
        Seq(
          schema,
          id,
          ref,
          title,
          description,
          `type`,
          properties,
          required,
          items,
          anyOf
        ).filterNot(_.equals(None)).map(_.get.toString).mkString(",") +
        "}"
    }

  }
  object JSS {
    @Override
    def apply(vs: Seq[JsonSchemaStructure]): JSS = {
      var schema: Option[ JSA_schema ] = None
      var id: Option[ JSA_id ] = None
      var ref: Option[ JSA_ref ] = None
      var title: Option[ JSA_title ] = None
      var description: Option[ JSA_description ] = None
      var `type`: Option[ JSA_type ] = None
      var properties: Option[ JSA_properties ] = None
      var required: Option[ JSA_required ] = None
      var items: Option[ JSA_items ] = None
      var anyOf: Option[ JSA_anyOf ] = None
      vs.foreach( jss => {
        jss match {
          case v: JSA_schema => schema = Some(v)
          case v: JSA_id => id = Some(v)
          case v: JSA_ref => ref = Some(v)
          case v: JSA_title => title = Some(v)
          case v: JSA_description => description = Some(v)
          case v: JSA_type => `type` = Some(v)
          case v: JSA_properties => properties = Some(v)
          case v: JSA_required => required = Some(v)
          case v: JSA_items => items = Some(v)
          case v: JSA_anyOf => anyOf = Some(v)
        }
      })
      return new JSS(
        schema = schema,
        id = id,
        ref = ref,
        title = title,
        description = description,
        `type` = `type`,
        properties = properties,
        required = required,
        items = items,
        anyOf = anyOf
      )
    }
  }

}