package org.rramchan.basic

import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.rramchan.basic.util.LogUtil
import org.apache.log4j.{Level, Logger}

object NetworkWordCounter {
  
  def main(args : Array[String]){
    if (args.length <2){
      System.err.println("Usage : NetworkWordCounter host port")
    
    System.exit(1)
  }
  LogUtil.setStreamingLogLevels();
    
  val conf = new SparkConf().setAppName("NetworkWordCounter").setMaster("local")
  val ssc = new StreamingContext(conf, Seconds(15))
  
  /*val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK)
  
  val words = lines.flatMap { x =>x.split(" ")}.map( words => (words,1)).reduceByKey(_+_)
  words.count();
  words.print();*/
  
  
   val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
  val log = Logger.getLogger(getClass.getName)
  log.error("TEST LOG")
  System.out.println(wordCounts.count())
   log.error(wordCounts.print())
    log.error(wordCounts.print().toString())
   
  
  ssc.start();
  ssc.awaitTermination();
  
  
  }
  
}