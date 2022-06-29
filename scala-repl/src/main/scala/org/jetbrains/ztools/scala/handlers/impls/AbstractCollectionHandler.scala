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

import org.jetbrains.ztools.scala.core.{Loopback, ResNames}
import org.jetbrains.ztools.scala.interpreter.ScalaVariableInfo

import scala.collection.mutable

abstract class AbstractCollectionHandler(limit: Int) extends AbstractTypeHandler {
  trait Iterator {
    def hasNext: Boolean

    def next: Any
  }

  def iterator(obj: Any): Iterator

  def length(obj: Any): Int

  override def handle(scalaInfo:  ScalaVariableInfo, loopback: Loopback, depth: Int): mutable.Map[String, Any] = mutable.Map[String, Any](
    ResNames.LENGTH -> length(scalaInfo.value),
    ResNames.VALUE -> withJsonArray { json =>
      val it = iterator(scalaInfo.value)
      var index = 0
      while (it.hasNext && index < limit) {
        val id = scalaInfo.path
        json += loopback.pass(it.next, s"$id[$index]")
        index += 1
      }
    }
  )
}
