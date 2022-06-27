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
package org.jetbrains.ztools.scala.handlers

import org.jetbrains.ztools.scala.core.Loopback

import scala.collection.mutable

abstract class AbstractCollectionHandler(limit: Int) extends AbstractTypeHandler {
  trait Iterator {
    def hasNext: Boolean

    def next: Any
  }

  def iterator(obj: Any): Iterator

  def length(obj: Any): Int

  override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String,Any] = withJsonObject {
    json =>
      json+=("jvm-type"-> obj.getClass.getCanonicalName)
      json+=("length"-> length(obj))
      json+=("value"-> withJsonArray { json =>
        val it = iterator(obj)
        var index = 0
        while (it.hasNext && index < limit) {
          json+=(extract(loopback.pass(it.next, s"$id[$index]")))
          index += 1
        }
      })
  }
}
