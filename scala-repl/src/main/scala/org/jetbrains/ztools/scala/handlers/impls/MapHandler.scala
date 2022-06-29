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
package org.jetbrains.ztools.scala.handlers.impls

import org.jetbrains.ztools.scala.core.Loopback
import org.jetbrains.ztools.scala.interpreter.ScalaVariableInfo

import scala.collection.mutable

class MapHandler(limit: Int, timeout: Int) extends AbstractTypeHandler {
  override def handle(scalaInfo: ScalaVariableInfo, loopback: Loopback, depth: Int): mutable.Map[String, Any] =
    withJsonObject {
      json =>
        val obj = scalaInfo.value
        val id = scalaInfo.path
        val map = obj.asInstanceOf[Map[_, _]]
        val keys = mutable.MutableList[Any]()
        val values = mutable.MutableList[Any]()
        json += ("jvm-type" -> obj.getClass.getCanonicalName)
        json += ("length" -> map.size)
        var index = 0

        json += ("key" -> keys)
        json += ("value" -> values)

        val startTime = System.currentTimeMillis()
        map.view.take(math.min(limit, map.size)).foreach {
          case (key, value) =>
            if (checkTimeoutError(scalaInfo.path, startTime, timeout))
              return json
            keys += loopback.pass(key, s"$id.key[$index]")
            values += loopback.pass(value, s"$id.value[$index]")
            index += 1
        }
    }

  override def accept(obj: Any): Boolean = obj.isInstanceOf[Map[_, _]]
}
