package org.apache.spark.examples

import org.apache.spark.sql.SparkSession


/**
  * Created by Pan on 2017/6/30.
  */
object WordCount {

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: WordCount <file>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("word count")
      .getOrCreate()

  /**
    *
    * 首先调用hadoopFile()，会创建一个HadoopRDD，其实是(key,value) pair，key是文本文件或hdfs文件的offset，
    * value是文本行。
    * 之后调用map()，将HadoopRDD变成MapPartitionsRDD，其内部就是一行一行的文本.
    *
    */
    val sourceRDD = spark.sparkContext.textFile(args(0))
    // flatMap MapPartitionsRDD-> MapPartitionsRDD
    val wordsRDD = sourceRDD.flatMap(_.split(' '))
    // map MapPartitionsRDD-> MapPartitionsRDD
    val tupleRDD = wordsRDD.map((_, 1))
    /**
      * 注意:在RDD中是没有reduceByKey(func)方法的，所以，会触发在RDD中定义的隐式转换
      * implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
      *   (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
      *   new PairRDDFunctions(rdd)
      *  }
      *  会新建一个PairRDDFunctions，从PairRDDFunctions类中去找reduceByKey(func)的方法。
      */
    val wordcountRDD = tupleRDD.reduceByKey(_ + _)

    // foreach是一个action
    wordcountRDD.foreach(f => println(f))
  }
}
