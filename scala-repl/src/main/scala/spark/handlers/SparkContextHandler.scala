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

import org.apache.spark.SparkContext
import org.jetbrains.ztools.scala.core.{Loopback, Names}
import org.jetbrains.ztools.scala.handlers.AbstractTypeHandler

import scala.collection.mutable

class SparkContextHandler extends AbstractTypeHandler {
  override def accept(obj: Any): Boolean = obj.isInstanceOf[SparkContext]

  override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] = withJsonObject {
    json =>
      val sc = obj.asInstanceOf[SparkContext]
      json += (Names.VALUE -> withJsonObject { json =>
        json += ("sparkUser" -> wrap(sc.sparkUser, "String"))
        json += ("sparkTime" -> wrap(sc.startTime, "Long"))
        json += ("applicationId()" -> wrap(sc.applicationId, "String"))
        json += ("applicationAttemptId()" -> wrap(sc.applicationAttemptId.toString, "Option[String]"))
        json += ("appName()" -> sc.appName)
      })
  }
}
