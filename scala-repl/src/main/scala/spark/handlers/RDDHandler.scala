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

import org.apache.spark.rdd.RDD
import org.jetbrains.ztools.scala.core.{Loopback, ResNames}
import org.jetbrains.ztools.scala.handlers.impls.AbstractTypeHandler
import org.jetbrains.ztools.scala.interpreter.ScalaVariableInfo

import scala.collection.mutable

class RDDHandler extends AbstractTypeHandler {
  override def accept(obj: Any): Boolean = obj.isInstanceOf[RDD[_]]

  override def handle(scalaInfo: ScalaVariableInfo, loopback: Loopback, depth: Int): mutable.Map[String, Any] = withJsonObject {
    json =>
      val obj = scalaInfo.value
      val rdd = obj.asInstanceOf[RDD[_]]
      json += (ResNames.VALUE -> withJsonObject { value =>
        value += ("getNumPartitions()" -> wrap(rdd.getNumPartitions, "Int"))
        value += ("name" -> wrap(rdd.name, "String"))
        value += ("id" -> wrap(rdd.id, "Int"))
        value += ("partitioner" -> wrap(rdd.partitioner.toString, "Option[org.apache.spark.Partitioner]"))
        value += ("getStorageLevel()" -> wrap(rdd.getStorageLevel.toString, "org.apache.spark.storage.StorageLevel"))
      })
  }
}
