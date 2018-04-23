package com.ijunhai.storage.greenplum

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}

import com.ijunhai.common.TimeUtil
import com.ijunhai.common.logsystem.{HaiwaiGoogle, HwVoidOrder, JunhaiLog}
import org.apache.tomcat.jdbc.pool.{DataSource, PoolProperties}
import org.bson.Document
import org.postgresql.util.PSQLException

import scala.collection.JavaConversions._


class MySQLSink(getConn: () => Connection) extends Serializable {
  lazy val connection = getConn()

  def insert(sql: String, database: String): Int = {
    val pstmt = connection.prepareStatement(sql)
    pstmt.executeUpdate()
  }

  def select(sql: String): ResultSet = {
    val pstmt = connection.prepareStatement(sql)
    pstmt.executeQuery()
  }

  def update(sql: String): Int = {
    val pstmt = connection.prepareStatement(sql)
    pstmt.executeUpdate()
  }

  def validateTableExist(table: String): Boolean = {
    var flag = true
    try {
      val meta = connection.getMetaData
      val type_ = Array("TABLE")
      val rs: ResultSet = meta.getTables(null, null, table, type_)
      flag = rs.next()
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    flag
  }


  def createAdvDetailTable(tableName: String): Unit = {
    val stmt = connection.createStatement

    //    val sql = "CREATE TABLE " + tableName +
    //      " (game varchar(50) NOT NULL DEFAULT '', " +
    //      "channel varchar(50) NOT NULL DEFAULT '', " +
    //      "junhai_adid varchar(50) NOT NULL DEFAULT '2', " +
    //      "client_ip varchar(20) NOT NULL DEFAULT '127.0.0.1', " +
    //      "company_id varchar(20) NOT NULL DEFAULT '', " +
    //      "click_time  varchar(50) NOT NULL DEFAULT '1970-07-01 00:00:00' " +
    //      ")DISTRIBUTED BY (client_ip); "


    val sql = "CREATE TABLE " + tableName +
      "( id varchar(50) PRIMARY KEY, " +
      "game varchar(50) NOT NULL DEFAULT '', " +
      "channel varchar(50) NOT NULL DEFAULT '', " +
      "junhai_adid varchar(50) NOT NULL DEFAULT '2', " +
      "client_ip varchar(20) NOT NULL DEFAULT '127.0.0.1', " +
      "company_id varchar(20) NOT NULL DEFAULT '', " +
      "click_time timestamp NOT NULL DEFAULT '1970-07-01 00:00:00', " +
      "click_source varchar(1024) NOT NULL DEFAULT '', " +
      "is_used smallint NOT NULL DEFAULT 0" +
      ")DISTRIBUTED BY (id); grant SELECT,UPDATE on public." + tableName + " to dalan_admin;"

    stmt.executeUpdate(sql)
  }

  def Select(document: Document, table: String): Int = {
    var num = 0;
    //println(document)
    val package_name = JunhaiLog.getString(document, HaiwaiGoogle.package_name)
    val purchase_token = JunhaiLog.getString(document, HaiwaiGoogle.purchaseToken)
    // println("传入的参数为"+package_name)
    // println("传入的token为"+purchase_token)
    //println(package_name+"1111")
    // println(purchase_token+"1111111")
    val sql = s"select count(1) from $table where pack_name='$package_name' and  purchase_token='$purchase_token'"
    // println(sql)
    val pstmt = connection.prepareStatement(sql)
    try {
      val result = pstmt.executeQuery()

      if (result.next()) {

        num = result.getInt("count");
        // println("结果集1"+num)

      }
      else {
        num = 0
      }
    } catch {
      case e: PSQLException =>
        //    e.printStackTrace()
        println("cause:" + e.getMessage)
    }
    // println("select num ="+num)
    num
  }

  def Update(document: Document, table: String): Int = {
    // println(document)
    // var num=0;
    var result = 0
    val mark = 1
    var voided_time = ""
    document.get("voidedTimeMillis") match {
      case value: java.sql.Date =>
        voided_time = TimeUtil.time2DateString("yyyy-MM-dd HH:mm:ss", value.getTime, TimeUtil.MILLISECOND)
    }
    val voided_date = JunhaiLog.getInt(document, HwVoidOrder.voided_time)
    val pack_name = JunhaiLog.getString(document, HaiwaiGoogle.package_name)
    val purchase_token = JunhaiLog.getString(document, HaiwaiGoogle.purchaseToken)
    val time_interval = JunhaiLog.getInt(document, "interval")
    val sql = s"update voided_order set mark=$mark,voided_time='$voided_time',voided_date=$voided_date,time_interval=$time_interval where pack_name='$pack_name' and  purchase_token='$purchase_token'"
    println(sql)
    //println(voided_time)
    val pstmt = connection.prepareStatement(sql)
    try {
      result = pstmt.executeUpdate()

    } catch {
      case e: PSQLException =>
        //    e.printStackTrace()
        println("cause:" + e.getMessage)
    }
    println("update =" + result)
    result
  }

  def insert(document: Document, tableTag: String): Document = {
    val table = if (tableTag.equals("detail")) {
      val date = TimeUtil.time2DateString("yyyyMMdd",
        document.get("click_time").asInstanceOf[java.sql.Date].getTime, TimeUtil.MILLISECOND)
      val tableName = tableTag + date
      if (!validateTableExist(tableName)) { //validateTableExist返回false 建表
        createAdvDetailTable(tableName)
      }
      tableName
    } else {
      tableTag
    }

    var sqlKey = ""
    var values = ""
    val keys = document.keySet()
    for (key <- keys) {
      sqlKey += key + ","
      document.get(key) match {
        case value: java.sql.Date =>
          values += "\'" + TimeUtil.time2DateString("yyyy-MM-dd HH:mm:ss", value.getTime, TimeUtil.MILLISECOND) + "\',"
        case _ =>
          values += "?,"
      }
    }

    sqlKey = sqlKey.substring(0, sqlKey.length - 1)
    values = values.substring(0, values.length - 1)
    val sql = s"insert into $table ($sqlKey) values($values)"
    val pstmt = connection.prepareStatement(sql)
    var index = 1
    for (key <- keys) {
      document.get(key) match {
        case value: Integer =>
          pstmt.setInt(index, value)
        case value: java.lang.Long =>
          pstmt.setLong(index, value)
        case value: String =>
          pstmt.setString(index, value)
        case value: java.lang.Double =>
          pstmt.setDouble(index, value)
        case value: java.sql.Date =>
          //                pstmt.setDate(index,value)
          index -= 1
        case _ =>
          pstmt.setObject(index, document.get(key))
      }
      index += 1
    }
    try {
      pstmt.executeUpdate()
    } catch {
      case e: PSQLException =>
        println("cause:" + e.getMessage)
    }
    JunhaiLog.documentClean2Json(document)
  }


  def insertBatch(document: Document, tableTag: String): Document = {
    val table = if (tableTag.equals("detail")) {
      val date = TimeUtil.time2DateString("yyyyMMdd",
        TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss", document.getString("click_time"), TimeUtil.MILLISECOND),
        TimeUtil.MILLISECOND)
      val tableName = tableTag + date
      if (!validateTableExist(tableName)) {
        createAdvDetailTable(tableName)
      }
      tableName
    } else tableTag

    val keys = document.keySet()
    val sql = "insert into t_product values";
    val pstmt = connection.prepareStatement(sql)
    var index = 1
    for (key <- keys) {
      pstmt.setString(index, document.get(key).toString)
      index += 1
    }
    try {
      pstmt.executeUpdate()
    } catch {
      case e: PSQLException =>
        println("cause:" + e.getMessage)
    }
    JunhaiLog.documentClean2Json(document)
  }


}

object MySQLSink {
  def apply(): MySQLSink = {
    val f = () => {

      val url = "jdbc:mysql://10.13.113.203:3306/jh_data"
      val user = "data_work"
      val password = "data_work#171012@#876"

      val poolProps = new PoolProperties
      poolProps.setDriverClassName("com.mysql.jdbc.Driver")
      poolProps.setUrl(url)
      poolProps.setUsername(user)
      poolProps.setPassword(password)
      poolProps.setTestOnBorrow(true)
      poolProps.setValidationQuery("select 1")
      val dataSource = new DataSource(poolProps)

      val conn = dataSource.getConnection

      sys.addShutdownHook {
        conn.close()
      }
      conn
    }
    new MySQLSink(f)
  }
}
