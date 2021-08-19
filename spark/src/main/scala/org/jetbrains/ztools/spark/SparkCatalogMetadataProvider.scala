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

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Row, SparkSession}
import org.jetbrains.bigdataide.shaded.org.json.{JSONArray, JSONObject}


class SparkCatalogMetadataProvider(spark: SparkSession) extends CatalogMetadataProvider {
  override def listTables(dbName: String): JSONArray = {
    val arr = new JSONArray()
    catalog.listTables(dbName).collect().foreach {
      table =>
        val json = new JSONObject()
          .put("isTemporary", table.isTemporary)
          .put("name", table.name)
          .put("description", table.description)
          .put("database", table.database)
          .put("tableType", table.tableType)

        arr.put(json)
    }
    arr
  }

  override def listFunctions(dbName: String): JSONArray = {
    val arr = new JSONArray()
    catalog.listFunctions(dbName).collect().foreach {
      func =>
        val json = new JSONObject()
          .put("className", func.className)
          .put("database", func.database)
          .put("description", func.description)
          .put("isTemporary", func.isTemporary)
          .put("name", func.name)
        arr.put(json)
    }
    arr
  }

  override def listColumns(dbName: String, tableName: String): JSONArray = {
    val arr = new JSONArray()
    val currentDatabase = catalog.currentDatabase
    catalog.setCurrentDatabase(dbName)
    try {
      catalog.listColumns(tableName).collect().foreach {
        column =>
          val json = new JSONObject()
            .put("name", column.name)
            .put("dataType", column.dataType)
            .put("description", column.description)
            .put("nullable", column.nullable)
            .put("isBucket", column.isBucket)
            .put("isPartition", column.isPartition)
          arr.put(json)
      }
      arr
    } finally {
      catalog.setCurrentDatabase(currentDatabase)
    }
  }

  override def listDatabases: JSONArray = {
    val arr = new JSONArray()
    catalog.listDatabases().collect().foreach {
      database =>
        val json = new JSONObject()
          .put("name", database.name)
          .put("locationUri", database.locationUri)
          .put("description", database.description)
        arr.put(json)
    }
    arr
  }

  override def toJson: JSONObject = {
    try {
      fromCatalog
    } catch {
      case e: Throwable =>
        try {
          fromSpark.put("exceptionFromCatalog", exceptionText(e))
        } catch {
          case e: Throwable =>
            new JSONObject().put("exceptionFromSql", exceptionText(e))
        }
    }
  }

  def fromCatalog: JSONObject = {
    val json = new JSONObject()
    val dbs = new JSONArray()
    val currentDatabase = catalog.currentDatabase
    try {
      catalog.listDatabases().collect().foreach {
        database =>
          val db = new JSONObject()
          val tbs = new JSONArray()
          catalog.setCurrentDatabase(database.name)
          catalog.listTables(database.name).collect().foreach {
            table =>
              val tb = new JSONObject()
              val cols = new JSONArray()
              catalog.listColumns(table.name).collect().foreach {
                column =>
                  cols.put(
                    new JSONObject()
                      .put("name", column.name)
                      .put("dataType", column.dataType)
                      .put("description", column.description)
                      .put("nullable", column.nullable)
                      .put("isBucket", column.isBucket)
                      .put("isPartition", column.isPartition))
              }
              tb.put("columns", cols)
                .put("isTemporary", table.isTemporary)
                .put("name", table.name)
                .put("description", table.description)
                .put("database", table.database)
                .put("tableType", table.tableType)
              tbs.put(tb)
          }
          db.put("tables", tbs)
            .put("name", database.name)
            .put("locationUri", database.locationUri)
            .put("description", database.description)
          dbs.put(db)
      }
    } finally {
      catalog.setCurrentDatabase(currentDatabase)
    }
    json.put("databases", dbs)
    json
  }

  def fromSpark: JSONObject = {
    val json = new JSONObject()
    val dbs = new JSONArray()
    spark.sql("show databases").collect().foreach {
      database =>
        val db = new JSONObject()
        val tbs = new JSONArray()
        findInSchema(database, "namespace").map {
          databaseName =>
            spark.sql(s"show tables from $databaseName").collect().foreach {
              table =>
                val tb = new JSONObject()
                val cols = new JSONArray()
                findInSchema(table, "tableName").map {
                  tableName =>
                    spark.sql(s"desc $databaseName.$tableName").collect().foreach {
                      column =>
                        val columnObject = new JSONObject()
                        findInSchema(column, "col_name").map { columnName => columnObject.put("name", columnName) }
                        findInSchema(column, "data_type").map { dataType => columnObject.put("dataType", dataType) }
                        cols.put(columnObject)
                    }
                    tb.put("columns", cols)
                    findBoolInSchema(table, "isTemporary").map{ isTemp => tb.put("isTemporary", isTemp) }
                    tb.put("name", tableName)
                    tbs.put(tb)
                }
            }
            db.put("tables", tbs)
            db.put("name", databaseName)
            dbs.put(db)
        }
    }
    json.put("databases", dbs)
    json
  }

  private def exceptionText(e: Throwable) = e.getStackTrace.mkString(System.lineSeparator())

  private def findBoolInSchema(row: Row, fieldName: String): Option[Boolean] = {
    row.schema.fields.zipWithIndex.find{ case (field: StructField, _) => field.name == fieldName }.map(_._2).flatMap{ index =>
      row(index) match {
        case b: Boolean => Some(b)
        case _ => None
      }
    }
  }

  private def findInSchema(row: Row, fieldName: String): Option[String] = {
    row.schema.fields.zipWithIndex.find { case (field: StructField, _) => field.name == fieldName }.map(_._2).map { index =>
      row(index).toString
    }
  }

  private val catalog = spark.catalog
}
