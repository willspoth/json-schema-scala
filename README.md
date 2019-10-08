# json-schema-scala
Calculate precision of json-schema and validate rows against a schema  
Main is in src/main/scala/JsonSchemaParser.scala

## Args
First arg: path to json-schema: /home/user/schemas/main.schema.json

Flags:  
-prec boolean | default: true | calculates precision  
-val file | valid Json string | validates string/ each line of a file / also works for directories
-logLevel | default: INFO | set's slf4j root log level

example args:  
1) /home/me/test.schema.json -val /home/me/test.dataset.json
2) /home/me/test.schema.json -prec false -val /home/me/test.dataset.json
3) /home/me/test.schema.json -prec false -val {"test":"hello-world"} -logLevel DEBUG
4) /home/me/test.schema.json -prec false -val /home/me/validateDir