package org.rramchan.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object HelloWordCount {
  def main (args : Array[String]) =  {
    
    val config = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(config)
    System.setProperty("hadoop.home.dir","C:\\ITBox\\hadoop")
    val textFile = sc.textFile("README.md", 1)

    
    val result = textFile.flatMap( x => x.split(" ") ).map(word => (word,1)).reduceByKey(_+_).saveAsTextFile("output/output.txt")
    
    
    
    
  
    
    
    
  }
}