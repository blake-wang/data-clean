package com.ijunhai.storage.greenplum

import java.sql.{Connection, DriverManager}


object GreenPlumManager {

  def getConnect(database:String): Connection ={
    val url="jdbc:postgresql://udw.lb1vb3.m0.service.ucloud.cn:5432/"+database
    val user="admin"
    val password="birthDAY+-0230"
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection(url,user,password)
  }

  def query(sql:String,database:String): Unit ={
    val conn=GreenPlumManager.getConnect(database)
    val pstmt=conn.prepareStatement(sql)
    val rs = pstmt.executeQuery()
    //    println(rs)
    val col = rs.getMetaData().getColumnCount()
    println("============================")
    while (rs.next()) {
      for (i<- 1 to col) {
        print(rs.getString(i) + "\t")
      }
      System.out.println("")
    }
    System.out.println("============================")
  }

  def insert(sql:String,database:String): Int ={
    val conn=GreenPlumManager.getConnect(database)
    val pstmt=conn.prepareStatement(sql)
    pstmt.executeUpdate()
  }
}
