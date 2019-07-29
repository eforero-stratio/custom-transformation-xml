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

import scala.xml.XML

class RepartitionLiteTransformStepStreaming(
                                         sparkSession: SparkSession,
                                         streamingContext: StreamingContext,
                                         properties: Map[String, String]
                                       ) extends LiteCustomStreamingTransform(sparkSession, streamingContext, properties) {

  override def transform(inputData: Map[String, ResultStreamingData]): OutputStreamingTransformData = {

    println("Se inicia el custom de transformación XML con Streaming")


    val sparkS = sparkSession

    /*
    // Inicial
    val newStream = inputData.head._2.data.transform { rdd =>
      rdd.repartition(5)
    }
    */

    val xmlStream = inputData.head._2.data.transform { rdd =>




      val arrayString: Array[String] = rdd.map(line => line.mkString).collect()
      val rddString: RDD[String] = sparkS.sparkContext.parallelize(arrayString)
      rddString.map(line => Row(line))



    }

    OutputStreamingTransformData(xmlStream)

  }
}
