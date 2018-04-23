package com.ijunhai.common

import java.io.{BufferedReader, File, FileReader}
import java.util
import java.util.Date

import com.ijunhai.common.http.HttpClientUtil
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.storage.kafka.Save2Kafka
import org.bson.Document
import org.bson.json.JsonParseException


/**
  * Created by Admin on 2017-09-30.
  * 有问题
  */
object StreamingProtector {
  //nohup java -classpath /work/bigdata/app/protect.jar com.ijunhai.util.StreamingProtector /work/bigdata/app/protected-apps.properties m2:23188,m1:23188 10 >/work/bigdata/app/StreamingProtector1.log 2>&1 &
  //C:\Users\admin\Desktop\protected-apps.properties
  def main(args: Array[String]) {
    val MONITOR = "monitor"
    var count = 0
    val KILL_SUBMIT = "kill -9 `jps | grep SparkSubmit | awk '{print $1}'`"
    if (args.length < 3) {
      println("<CONFIG_PATH> <YARN_RESOURCE_MANAGER_HOST_AND_PORT> <INTERNEL_TIME(MINITE)> <kill_flag>")
      System.exit(1)
    }
    val KILL_FLAG: Boolean = if (args.length == 4) {
      args(3) == "true"
    } else {
      true
    }
    println("KILL_FLAG:" + KILL_FLAG)
    val hostAndPort = args(1).split(',')
    val masterURL1 = "http://" + hostAndPort(0) + "/ws/v1/cluster/apps"
    val masterURL2 = "http://" + hostAndPort(1) + "/ws/v1/cluster/apps"
    var masterIndex = 0
    val httpUtil = new HttpClientUtil
    while (true) {
      var result = httpUtil.doGet(masterURL1)
      //      if (!result.getContent.startsWith("{")) {
      if (result.getContent.contains("This is standby RM")) {
        masterIndex = 1
        result = httpUtil.doGet(masterURL2)
      }
      println(new Date() + s" Current host and port ${hostAndPort(masterIndex)}")
      if (result.getStatusCode == 200) {
        val producer = Save2Kafka.initProducer()
        val appNamesStr = txt2String(new File(args(0)))
        val appNames = appNamesStr.split("\n").filter(line => line != "" && line != "\r").map(line => {
          val name = line.substring(0, line.indexOf("="))
          val shell = line.substring(line.indexOf("=") + 1)
          (name, shell)
        }).toMap
        //      val appNames=new Properties()
        //      appNames.load(new FileInputStream(args(0)))
        try {
          val document = Document.parse(result.getContent)
          val apps = document.get("apps").asInstanceOf[Document].get("app").asInstanceOf[util.ArrayList[Document]]
          val results = for (i <- 0 until apps.size()) yield {
            (apps.get(i), apps.get(i).getString("state"))
          }
          val runApps = results.filter(line => {
            line._2 == "RUNNING" || line._2 == "ACCEPTED" || line._2 == "SUBMITTED" || line._2 == "UNDEFINED"
          }).toArray
          //      runApps.foreach(println)
          val runAppNames = runApps.map(line => {
            val appUrl = "http://" + hostAndPort(masterIndex) + "/proxy/" + line._1.getString("id")
            var result = httpUtil.doGet(appUrl)
            var i = 0
            while (result.getStatusCode != 200) {
              Thread.sleep(1000 * 10)
              i += 1
              println(new Date + s" get url $appUrl fail!")
              Save2Kafka.sendMetrics(producer, MONITOR, new Date(), StreamingProtector.getClass.getName, JunhaiLog.ERROR, s"Get URL $appUrl fail!")
              result = httpUtil.doGet(appUrl)
            }
            val startStr = "<title>"
            val endStr = "</title>"
            val str = " - Spark Jobs"
            val httpCode = result.getContent
            val endPos = httpCode.indexOf(endStr) - str.length
            val startPos = httpCode.indexOf(startStr) + startStr.length
            val json_str = if (endPos > 0 && endPos > startPos) {
              httpCode.substring(startPos, endPos)
            } else {
              ""
            }
            println(json_str)
            Save2Kafka.sendMetrics(producer, MONITOR, new Date(), StreamingProtector.getClass.getName, JunhaiLog.INFO, s"Find current run or accepted app name $json_str")
            json_str
          }).toSet
          //      runAppNames.foreach(appNames.remove)
          //      val shells=appNames.keySet().toArray.map(line=>{
          //        appNames.getOrDefault(line,"")
          //      })
          //      shells.foreach(println)
          val shells = appNames.keySet.diff(runAppNames).map(line => {
            appNames.getOrElse(line, "")
          })
          shells.foreach(cmd => {
            YarnManager.exec(cmd)
            count += 1
            if (count > 3 && KILL_FLAG) {
              YarnManager.exec(KILL_SUBMIT) //清除本地的submit进程
            }
            println(new Date() + " " + cmd)
            Save2Kafka.sendMetrics(producer, MONITOR, new Date(), StreamingProtector.getClass.getName, JunhaiLog.WARM, s"Start app cmd $cmd")
          })
        } catch {
          case e: JsonParseException =>
            Save2Kafka.sendMetrics(producer, MONITOR, new Date(), StreamingProtector.getClass.getName, JunhaiLog.ERROR, s"JsonParseException:${result.getContent}")
        }
        Save2Kafka.closeProducer(producer)

        Thread.sleep(1000 * 60 * args(2).toInt)
      } else {
        println(new Date() + s" Wrong status code ${result.getStatusCode}!")
      }
      //      System.exit(0)
      Thread.sleep(1000 * 10)
    }
  }

  /**
    * 读取txt文件的内容
    *
    * @param file 想要读取的文件对象
    * @return 返回文件内容
    */
  def txt2String(file: File): String = {
    val result = new StringBuilder()
    try {
      val br = new BufferedReader(new FileReader(file));
      //构造一个BufferedReader类来读取文件
      var s = br.readLine()
      while (s != null) {
        //使用readLine方法，一次读一行
        result.append(System.lineSeparator() + s)
        s = br.readLine()
      }
      br.close();
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    result.toString()
  }
}
