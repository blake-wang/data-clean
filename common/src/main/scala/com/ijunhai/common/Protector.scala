package com.ijunhai.common

import java.io._
import java.util
import java.util.Date

import scala.collection.JavaConversions._

object Protector {
  //nohup java -classpath /data/agent_jar/protect.jar com.ijunhai.common.Protector   /data/protect/protected-apps.properties  true > /data/log/StreamingProtector.log  2>&1 &
  def main(args: Array[String]): Unit = {
    val KILL_SUBMIT = "kill -9 `jps | grep SparkSubmit | awk '{print $1}'`"
    var count = 0
    val KILL_FLAG = if (args(1).equals("true")) true else false
    while (true) {
      println("----- " + new Date() + " -----")
      val runAppNames: util.HashSet[String] = new util.HashSet[String]
      try {
        val cmdA = Array("/bin/sh", "-c", "yarn application -list | /usr/bin/awk '{print substr($0,32,37)}'")
        val process = Runtime.getRuntime.exec(cmdA)
        val input: BufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream))
        val runApps: util.HashSet[String] = YarnManager.get(runAppNames, input)
        //获取配置
        val appNamesStr = txt2String(new File(args(0)))
        val appNames = appNamesStr.split("\n").filter(line => line != "" && line != "\r").map(line => {
          val name = line.substring(0, line.indexOf("="))
          val shell = line.substring(line.indexOf("=") + 1)
          (name, shell)
        }).toMap

        val shells = appNames.keySet.diff(runApps).map(line => {
          appNames.getOrElse(line, "")
        })
        shells.foreach(cmd => {
          YarnManager.exec(cmd)
          count += 1
          if (count > 3 && KILL_FLAG) YarnManager.exec(KILL_SUBMIT) //清除本地的submit进程
          println(cmd)
        })
        input.close()
      } catch {
        case e: IOException =>
          e.printStackTrace()
      }
      Thread.sleep(1000 * 60 * 10)
    }
  }

  def txt2String(file: File): String = {
    val result = new StringBuilder()
    try {
      val br = new BufferedReader(new FileReader(file))
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
