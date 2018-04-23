package com.ijunhai.storage.hbase


import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{Filter, FilterList, SingleColumnValueFilter, SubstringComparator}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, TableName}

import scala.collection.JavaConverters._

class HbaseSink(getConn: () => Connection) extends Serializable {
  lazy val hbaseConn = getConn()

  def insert(tableName: String, rowKey: String, cf: String, qulified: String, value: String): Unit = {
    val table = hbaseConn.getTable(TableName.valueOf(tableName))
    val p = new Put(rowKey.getBytes())
    p.addColumn(cf.getBytes, qulified.getBytes, value.getBytes)
    //put 'hbase_test', '005','cf:money','money5'
    table.put(p)
    table.close()
  }

  def get(tableName: String, rowKey: String, cf: String, qulified1: String, qulified2: String): util.HashMap[String, String] = {
    val table = hbaseConn.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowKey))
    get.addFamily(Bytes.toBytes(cf))
    val result = table.get(get)
    val map = new util.HashMap[String, String]()
    val after1 = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(qulified1))
    val after2 = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(qulified2))
    val value1 = if (after1 == null) "" else new String(after1)
    val value2 = if (after2 == null) "" else new String(after2)
    map.put(qulified1, value1)
    map.put(qulified2, value2)
    map
  }


  def getFilterValue(tableName: String, arr: util.List[String]): Unit = {
    val table = hbaseConn.getTable(TableName.valueOf(tableName))

    var filterList = new util.ArrayList[Filter]()
    //创建一个list来存放多个filter，默认下这些组合是“与”，”或“的话需注明
    //创建一个Scan
    val sc = new Scan()
    for (str <- arr.asScala) {
      var arrS = str.split(",")
      filterList.add(new SingleColumnValueFilter(arrS(0).getBytes(), arrS(1).getBytes(), CompareOp.EQUAL, new SubstringComparator(arrS(2))))
      sc.addColumn(str(0).toString.getBytes(), str(1).toString.getBytes()) //在这里循环添加需要添加的列
    }
    //添加过滤，只返回想要列
    sc.addColumn("cf".getBytes(), "money".getBytes())
    //将list添加到FilterList中
    val fl = new FilterList(filterList)
    sc.setFilter(fl)
    //定义一个ResultScanner来接收返回的值
    val ResultFilterValue: ResultScanner = table.getScanner(sc)
    for (r <- ResultFilterValue.asScala) {
      println(r)
    }
  }

  def close(): Unit = {
    if (hbaseConn!=null) {
      hbaseConn.close()
    }
  }

}

object HbaseSink {
  def apply(): HbaseSink = {
    val f = () => {
      val conf = HBaseConfiguration.create()
//            conf.set("hbase.rootdir", "hdfs://slave-02:8020/hbase")
//            conf.set("hbase.zookeeper.quorum", "slave-01,slave-02,slave-03")
//
      conf.set("hbase.rootdir", "hdfs://Ucluster/hbase")
      conf.set("hbase.zookeeper.quorum", "uhadoop-1neej2-master1:2181,uhadoop-1neej2-master2:2181,uhadoop-1neej2-core1:2181")

      //      conf.set("hbase.zookeeper.quorum", "10.13.82.206,10.13.55.188,10.13.171.219")
      val conn: Connection = ConnectionFactory.createConnection(conf)

      sys.addShutdownHook {
        conn.close()
      }
      conn
    }
    new HbaseSink(f)
  }

  def apply(conf: Configuration): HbaseSink = {
    val f = () => {
      val conn: Connection = ConnectionFactory.createConnection(conf)

      sys.addShutdownHook {
        conn.close()
      }
      conn
    }
    new HbaseSink(f)
  }

}
