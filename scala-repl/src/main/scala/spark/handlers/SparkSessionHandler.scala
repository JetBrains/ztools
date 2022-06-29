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

import org.apache.spark.sql.SparkSession
import org.jetbrains.ztools.scala.core.{Loopback, ResNames}
import org.jetbrains.ztools.scala.handlers.impls.AbstractTypeHandler
import org.jetbrains.ztools.scala.interpreter.ScalaVariableInfo

import scala.collection.mutable

class SparkSessionHandler extends AbstractTypeHandler {
  override def accept(obj: Any): Boolean = obj.isInstanceOf[SparkSession]

  override def handle(scalaInfo: ScalaVariableInfo, loopback: Loopback, depth: Int): mutable.Map[String, Any] = withJsonObject {
    json =>
      val obj = scalaInfo.value
      val id = scalaInfo.path

      val spark = obj.asInstanceOf[SparkSession]
      json += (ResNames.VALUE -> withJsonObject { json =>
        json += ("version()" -> spark.version)
        json += ("sparkContext" -> loopback.pass(spark.sparkContext, s"$id.sparkContext"))
        json += ("sharedState" -> loopback.pass(spark.sharedState, s"$id.sharedState"))
      })
  }
}
