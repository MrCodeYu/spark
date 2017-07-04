package org.apache.spark.examples.streaming

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.feature.HashingTF

/**
  * Created by Pan on 2017/6/12.
  */
object Test {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setMaster("local[4]")

    val sc = new SparkContext(conf)

    val spam = sc.textFile("")
    val ham = sc.textFile("")

    val tf = new HashingTF()



  }
}
