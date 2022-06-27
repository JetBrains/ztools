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
import org.jetbrains.ztools.scala.core.{Loopback, Names}
import org.jetbrains.ztools.scala.handlers.AbstractTypeHandler

import scala.collection.mutable

class DatasetHandler extends AbstractTypeHandler {
  override def accept(obj: Any): Boolean = obj.isInstanceOf[Dataset[_]]

  override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] = withJsonObject {
    json =>
      val df = obj.asInstanceOf[Dataset[_]]
      json += (Names.VALUE -> withJsonObject { json =>
        json += ("schema()" -> wrap(withJsonArray {
          schema =>
            df.schema.zipWithIndex.foreach {
              case (field, index) =>
                schema += (extract(loopback.pass(field, s"$id.schema()[${index}]")))
            }
        }, "org.apache.spark.sql.types.StructType"))
        json += ("getStorageLevel()" -> wrap(df.storageLevel.toString(), "org.apache.spark.storage.StorageLevel"))
      })
  }
}
