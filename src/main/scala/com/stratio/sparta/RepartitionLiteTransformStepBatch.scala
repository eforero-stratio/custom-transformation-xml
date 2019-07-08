/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package com.stratio.sparta

import java.io.File

import com.stratio.sparta.sdk.lite.batch._
import com.stratio.sparta.sdk.lite.batch.models._
import org.apache.spark.sql._
import com.databricks.spark.xml.XmlReader

import scala.xml.{Node, XML}

class RepartitionLiteTransformStepBatch(
                                         sparkSession: SparkSession,
                                         properties: Map[String, String]
                                       ) extends LiteCustomBatchTransform(sparkSession, properties) {

  override def transform(inputData: Map[String, ResultBatchData]): OutputBatchTransformData = {

    val sc = sparkSession.sparkContext
    //val rowTag = properties.get("rowTag").getOrElse("")
    val inputStream = inputData.head._2.data.map(_.mkString)
    val stringInputStream: String = inputStream.reduce((x, y)=>x+y)
    val allNodes: Seq[Node] = XML.loadString(stringInputStream).child
    val tagsList: Seq[String] = allNodes.map(node => node.toString())
    val xmlStringRDD = sc.parallelize(tagsList)

    //val df = new XmlReader().withRowTag(rowTag).xmlRdd(sparkSession.sqlContext, xmlStringRDD).rdd
    val df = new XmlReader().xmlRdd(sparkSession.sqlContext, xmlStringRDD).rdd

    OutputBatchTransformData(df.repartition(5))

  }



}
