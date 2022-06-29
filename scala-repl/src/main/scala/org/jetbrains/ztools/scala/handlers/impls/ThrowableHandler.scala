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

import java.io.{PrintWriter, StringWriter}
import scala.collection.mutable

class ThrowableHandler extends AbstractTypeHandler {
  override def accept(obj: Any): Boolean = obj.isInstanceOf[Throwable]

  override def handle(scalaInfo:  ScalaVariableInfo, loopback: Loopback, depth: Int): mutable.Map[String, Any] = {
    val obj = scalaInfo.value
    val throwable = obj.asInstanceOf[Throwable]
    val writer = new StringWriter()
    val out = new PrintWriter(writer)
    throwable.printStackTrace(out)

    mutable.Map(
      ResNames.VALUE -> writer.toString
    )
  }
}
