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

import org.apache.spark.sql.catalyst.catalog.ExternalCatalog
import org.jetbrains.bigdataide.shaded.org.json.{JSONArray, JSONObject}

class ExternalCatalogMetadataProvider(catalog: => ExternalCatalog) extends CatalogMetadataProvider {
  override def listTables(dbName: String): JSONArray = {
    val result = new JSONArray()
    catalog.listTables(dbName).foreach {
      name =>
        val table = catalog.getTable(dbName, name)
        result.put(new JSONObject().put("name", name)
          .put("database", table.database)
          .put("owner", table.owner)
        )
    }
    result
  }

  override def listFunctions(dbName: String): JSONArray = {
    val result = new JSONArray()
    catalog.listFunctions(dbName, null).foreach {
      name =>
        val function = catalog.getFunction(dbName, name)
        result.put(new JSONObject().put("name", name)
          .put("className", function.className)
        )
    }
    result
  }

  override def listColumns(dbName: String, tableName: String): JSONArray = {
    val table = catalog.getTable(dbName, tableName)
    val result = new JSONArray()
    table.schema.fields.foreach {
      column =>
        val json = new JSONObject()
          .put("name", column.name)
          .put("dataType", column.dataType.toString)
          .put("nullable", column.nullable)
        result.put(json)
    }
    result
  }

  override def listDatabases: JSONArray = {
    val result = new JSONArray()
    catalog.listDatabases.foreach {
      name =>
        val database = catalog.getDatabase(name)
        result.put(new JSONObject().put("name", name)
          .put("locationUri", database.locationUri)
          .put("description", database.description)
        )
    }
    result
  }

  override def toJson: JSONObject = {
    throw new UnsupportedOperationException("Not implemented yet")
  }
}
