/*
 *
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.spark

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import java.sql.{Connection, Date, DriverManager, Timestamp}
import java.util.Properties
import java.util.regex.Pattern

import org.clapper.classutil.ClassFinder

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

class JDBCWrapper(prop: Properties, databaseName: Option[String] = Option.empty) {
  val Sep: String = "|"
  private val createdDBs = ArrayBuffer.empty[String]


  val connection: Connection = {
    val jdbcUsername = prop.getProperty("tidbuser")
    val jdbcHostname = prop.getProperty("tidbaddr")
    val jdbcPort = Integer.parseInt(prop.getProperty("tidbport"))
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}?user=${jdbcUsername}"

    DriverManager.getConnection(jdbcUrl, jdbcUsername, "")
  }

  switchDB(databaseName)

  def TryResource[T](res: T) (closeOp: T => Unit) (taskOp: T => Unit): Unit = {
    try {
      taskOp(res)
    } finally {
      closeOp(res)
    }
  }

  def dumpFile(content: String, path: String): Unit = {
    TryResource(new PrintWriter(path)) (_.close()) { _.print(content) }
  }

  def readFile(path: String): List[String] = {
    Files.readAllLines(Paths.get(path)).toList
  }

  def dumpCreateTable(table: String, path: String): Unit = {
    val (_, res) = queryTiDB("show create table " + table)
    val content = res(0)(1).toString
    dumpFile(content, path)
  }

  def valToString(value: Any): String = value.toString

  def valFromString(str: String, tp: String): Any = {
    tp match {
      case "VARCHAR" | "CHAR" | "TEXT" => str
      case "FLOAT" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "DECIMAL" | "NUMERIC" => BigDecimal(str)
      case "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INTEGER" | "BIGINT" => str.toLong
      case "DATE" => Date.valueOf(str)
      case "TIME" | "TIMESTAMP" | "DATETIME" => Timestamp.valueOf(str)
    }
  }

  def rowToString(row: List[Any]): String = row.map(valToString).mkString(Sep)

  def rowFromString(row: String, types: List[String]): List[Any] = {
    row.split(Pattern.quote(Sep)).zip(types).map {
      case (value, colType) => valFromString(value, colType)
    }.toList
  }

  def dumpTableContent(table: String, path: String): Unit = {
    val query = s"select * from ${table}"
    val (schema, result) = queryTiDB(query)
    val content = schema.mkString(Sep) + "\n" + result.map(rowToString).mkString("\n")
    dumpFile(content, path)
  }

  def createTable(path: String) = {
    val statement = connection.createStatement()
    statement.executeUpdate(readFile(path).mkString("\n"))
  }

  def insertRow(row: List[Any], table: String): Unit = {
    val placeholders = List.fill(row.size)("?").mkString(",")
    val stat = s"insert into ${table} values (${placeholders})"
    val ps = connection.prepareStatement(stat)
    row.zipWithIndex.map { case (value, index) => {
        value match {
          case f: Float => ps.setFloat(index + 1, f)
          case d: Double => ps.setDouble(index + 1, d)
          case bd: BigDecimal => ps.setBigDecimal(index + 1, bd.bigDecimal)
          case b: Boolean => ps.setBoolean(index + 1, b)
          case s: Short => ps.setShort(index + 1, s)
          case i: Int => ps.setInt(index + 1, i)
          case l: Long => ps.setLong(index + 1, l)
          case d: Date => ps.setDate(index + 1, d)
          case ts: Timestamp => ps.setTimestamp(index + 1, ts)
        }
      }
    }
    ps.executeUpdate()
  }

  def loadData(path: String, table: String): Unit = {
    val lines = readFile(path)
    val (schema, rows) = (lines(0).split(Pattern.quote(Sep)).toList, lines.drop(1))
    val rowData: List[List[Any]] = rows.map { rowFromString(_, schema) }
    rowData.map(insertRow(_, table))
  }

  private def switchDB(databaseName: Option[String]): Unit = {
    val db: String = databaseName.getOrElse {
      val sandbox = "sandbox_" + System.nanoTime()
      createDatabase(sandbox)
      sandbox
    }

    connection.setCatalog(db)
  }

  def createDatabase(dbName: String): Unit = {
    val statement = connection.createStatement()
    statement.executeUpdate("create database " + dbName)
    createdDBs.append(dbName)
  }

  private def dropDatabases(): Unit = {
    createdDBs.foreach { dbName =>
      val statement = connection.createStatement()
      statement.executeUpdate("drop database " + dbName)
    }
  }

  def queryTiDB(query: String): (List[String], List[List[Any]]) = {
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(query)
    val rsMetaData = resultSet.getMetaData();
    val retSet = ArrayBuffer.empty[List[Any]]
    val retSchema = ArrayBuffer.empty[String]
    for (i <- 1 to rsMetaData.getColumnCount) {
      retSchema += rsMetaData.getColumnTypeName(i)
    }
    while (resultSet.next()) {
      val row = ArrayBuffer.empty[Any]

      for (i <- 1 to rsMetaData.getColumnCount) {
        row += resultSet.getObject(i)
      }
      retSet += row.toList
    }
    (retSchema.toList, retSet.toList)
  }

  def close(): Unit = {
    dropDatabases
    connection.close
  }
}

object TestFramework111 {

  def main(args: Array[String]) = {
    val prop: Properties = TestFramework.loadConf("config.properties")
    val path = prop.getProperty("testbasepath")
    val dir = Paths.get(path, "tpch")
    dir.toFile.mkdirs()
    dir.resolve("part")
    val w = new JDBCWrapper(prop)
    w.dumpCreateTable("tr", dir.resolve("tr.ddl").toString)
    w.dumpTableContent("tr", dir.resolve("tr.data").toString)

    w.createTable(dir.resolve("tr.ddl").toString)
    w.loadData(dir.resolve("tr.data").toString, "tr")
  }
}
