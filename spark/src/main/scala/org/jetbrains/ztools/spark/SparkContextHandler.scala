/**
 * Copyright 2020 Jetbrains
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetbrains.ztools.spark

import org.apache.spark.SparkContext
import org.jetbrains.ztools.core.{Loopback, Names}
import org.jetbrains.ztools.scala.AbstractTypeHandler
import org.jetbrains.bigdataide.shaded.org.json.JSONObject

class SparkContextHandler extends AbstractTypeHandler {
  override def accept(obj: Any): Boolean = obj.isInstanceOf[SparkContext]

  override def handle(obj: Any, id: String, loopback: Loopback): JSONObject = withJsonObject {
    json =>
      val sc = obj.asInstanceOf[SparkContext]
      json.put(Names.VALUE, withJsonObject { json =>
        json.put("sparkUser", wrap(sc.sparkUser, "String"))
        json.put("sparkTime", wrap(sc.startTime, "Long"))
        json.put("applicationId()", wrap(sc.applicationId, "String"))
        json.put("applicationAttemptId()", wrap(sc.applicationAttemptId.toString, "Option[String]"))
        json.put("appName()", sc.appName)
      })
  }
}
