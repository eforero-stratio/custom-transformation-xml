/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package com.stratio.sparta


import com.databricks.spark.xml.XmlReader
import com.stratio.sparta.sdk.lite.streaming._
import com.stratio.sparta.sdk.lite.streaming.models._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.xml.XML

class RepartitionLiteTransformStepStreaming(
                                             sparkSession: SparkSession,
                                             streamingContext: StreamingContext,
                                             properties: Map[String, String]
                                           ) extends LiteCustomStreamingTransform(sparkSession, streamingContext, properties) {

  override def transform(inputData: Map[String, ResultStreamingData]): OutputStreamingTransformData = {

    println("Se inicia el custom de transformación XML con Streaming")

    val sparkS = sparkSession

    val xmlStream = inputData.head._2.data.transform { rdd =>
      val xmlString = rdd.map(line=>line.mkString).collect.mkString
      val allNodes = XML.loadString(xmlString).child
      val tagsList: Seq[String] = allNodes.map(node => node.mkString)
      val xmlStringRDD: RDD[String] = sparkS.sparkContext.parallelize(tagsList)
      new XmlReader().xmlRdd(sparkS.sqlContext, xmlStringRDD).rdd
    }

    OutputStreamingTransformData(xmlStream)

  }
}
