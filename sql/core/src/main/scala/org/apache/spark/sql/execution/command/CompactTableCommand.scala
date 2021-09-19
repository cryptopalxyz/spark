/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.spark.sql.execution.command

import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.TableIdentifierContext


/**
 * Alter a table partition's spec.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table PARTITION spec1 RENAME TO PARTITION spec2;
 * }}}
 */
case class CompactTableCommand(
                                tableName: TableIdentifierContext,
                                fileNum: Int
                              ) extends LeafRunnableCommand {

  val tableIdentifier = TableIdentifier(tableName.table.getText, Some(tableName.db.getText))

  override def run(sparkSession: SparkSession): Seq[Row] = try {
    val table = sparkSession.catalog.getTable(tableIdentifier.database.get, tableIdentifier.table)

    val location = sparkSession.sql("desc formatted data_db.part_table")
      .collect()
      .filter(r => r(0).equals("Location"))
      .map(r => r(1))
      .mkString

    // temp folder for result
    val tmpOutputPath = new Path(location, UUID.randomUUID().toString()).toString
    print(tmpOutputPath)

    val df = sparkSession.sql("select * from " + table.database + "." + table.name)



    // compare directory size and folder size
    val hdfs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val blockSize = hdfs.getDefaultBlockSize(new Path(location))
    val directorySize = hdfs.getContentSummary(new Path(location)).getLength
    if (directorySize <= blockSize) {
      df.repartition(1).write.mode("overwrite").save(tmpOutputPath)
    }
    df.repartition(fileNum).write.mode("overwrite").save(tmpOutputPath)

    // rename the tmp path to current path
    hdfs.rename(new Path(tmpOutputPath), new Path(location))



    Seq.empty[Row]
  } catch { case NonFatal(cause) =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n").map(Row(_))
  }


}



