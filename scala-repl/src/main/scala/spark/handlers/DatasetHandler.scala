/**
 * Copyright 2020 Jetbrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package spark.handlers

import org.apache.spark.sql.Dataset
import org.jetbrains.ztools.scala.core.{Loopback, ResNames}
import org.jetbrains.ztools.scala.handlers.impls.AbstractTypeHandler
import org.jetbrains.ztools.scala.interpreter.ScalaVariableInfo

import scala.collection.mutable

class DatasetHandler extends AbstractTypeHandler {
  override def accept(obj: Any): Boolean = obj.isInstanceOf[Dataset[_]]

  override def handle(scalaInfo: ScalaVariableInfo, loopback: Loopback, depth: Int): mutable.Map[String, Any] = {
    val obj = scalaInfo.value
    val df = obj.asInstanceOf[Dataset[_]]


    val jsonSchema = df.schema.fields.map(field =>
      withJsonObject { jsonField =>
        jsonField += "name" -> wrap(field.name, null)
        jsonField += "nullable" -> wrap(field.nullable, null)
        jsonField += "dataType" -> wrap(field.dataType.typeName, null)
      }
    )

    val dfValue = mutable.Map(
      "schema()" -> wrap(jsonSchema, "org.apache.spark.sql.types.StructType"),
      "getStorageLevel()" -> wrap(df.storageLevel.toString(), "org.apache.spark.storage.StorageLevel")
    )

    mutable.Map(
      ResNames.VALUE -> dfValue
    )
  }
}
