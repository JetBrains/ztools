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
package org.jetbrains.ztools.spark

import org.jetbrains.bigdataide.shaded.org.json.{JSONArray, JSONObject}

trait CatalogMetadataProvider {
  def listTables(dbName: String): JSONArray

  def listFunctions(dbName: String): JSONArray

  def listColumns(dbName: String, tableName: String): JSONArray

  def listDatabases: JSONArray

  def toJson: JSONObject
}
