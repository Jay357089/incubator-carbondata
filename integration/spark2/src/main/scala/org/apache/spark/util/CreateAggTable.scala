package org.apache.spark.util

/**
  * Created by l00357089 on 2016/12/1.
  */
object CreateAggTable {

  // args[0]: agg table name, args[1]  query sql file.
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: query sql file is needed")
      System.exit(1)
    }

  }
}