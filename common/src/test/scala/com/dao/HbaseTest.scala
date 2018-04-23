package com.dao

import java.util.Date

import com.ijunhai.storage.hbase.HbaseSink
/**
  * Created by admin on 2018/1/18.
  */
object HbaseTest {
  def main(args: Array[String]): Unit = {
    println(new Date().getTime)

    val hbaseSink = HbaseSink.apply()
    hbaseSink.insert("hbase_test","rowkey2", "cf","date","test1")
    val arr = new java.util.ArrayList[String]
//    arr.add("cf,date,0111")
//    arr.add("cf,date,0112")
//    hbaseSink.getFilterValue("hbase_test",arr)
  }

}
