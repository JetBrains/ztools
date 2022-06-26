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

import org.codehaus.jettison.json.{JSONArray, JSONObject}
import org.jetbrains.ztools.scala.core.{Names, TypeHandler}

import scala.collection.JavaConversions.asScalaIterator

abstract class AbstractTypeHandler extends TypeHandler {
  protected def withJsonArray(body: JSONArray => Unit): JSONArray = {
    val arr = new JSONArray()
    body(arr)
    arr
  }

  protected def withJsonObject(body: JSONObject => Unit): JSONObject = {
    val obj = new JSONObject()
    body(obj)
    obj
  }

  protected def extract(json: JSONObject): Any =
    if (json.keys().size == 1) json.get("value") else json

  protected def wrap(obj: Any, tpe: String): JSONObject = withJsonObject {
    json =>
      json.put(Names.VALUE, obj)
      json.put(Names.TYPE, tpe)
  }
}