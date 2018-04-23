package com.ijunhai.common

import java.util.Date

import com.ijunhai.common.http.{HttpClientUtil, Result}

/**
  * Created by Admin on 2017-09-13.
  */
object GetTables {
  //java -classpath /work/bigdata/data-clean.jar com.ijunhai.GetTables
  def main(args: Array[String]) {
    //    val hdfsPath="hdfs://uhadoop-1neej2-master2/tmp/"
    val hdfsPath = "hdfs://uhadoop-1neej2-master1/tmp/"
    val map = Map("game_channel_id.json" -> "http://game.data.ijunhai.com/Gamedata/api/getAgentGameChannel", //独代拿包ID
      "agent_channel.json" -> "http://game.data.ijunhai.com/Gamedata/api/getAgentChannel",
      "sub_channel.json" -> "http://game.data.ijunhai.com/Gamedata/api/getAgentSubChannel",
      "agent_old_game_channel.json" -> "http://game.data.ijunhai.com/Gamedata/api/getAgentOldGameChannel",
      "agent_2_game_channel.json" -> "http://bigdata.ijunhai.com/api/getBelongGameToGameChannelId",
      "rate.json" -> "http://bigdata.ijunhai.com/api/getRate?date=20171103",
      "agent_game.json" -> "http://game.data.ijunhai.com/Gamedata/api/getAgentGame",
      "IOS128.json" -> "http://bigdata.ijunhai.com/api/getIOS128",
      "test_server.json" -> "http://bigdata.ijunhai.com/api/getTestServer"
    )

    map.foreach(line => {
      getContent(line._2, hdfsPath + line._1)
    })
  }

  /**
    * 获取http上的请求默认连接超时时间为3000毫秒，数据传输时间为4000毫秒
    * 如果http请求超时或者错误，则去上次缓存的地方拿，目前存在hdfs上
    * 拿到的话就覆盖hdfs上的内容
    * 如果hdfs上也没有就报错
    *
    * @param url       请求的url链接
    * @param cacheFile hdfs缓存文件
    * @return
    */
  def getContent(url: String, cacheFile: String): Unit = {
    val hcu = new HttpClientUtil()
    val result = try {
      hcu.doGet(url)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        System.err.println(new Date + " Error: get url=" + url + "fail")
        new Result()
    }
    if (result.getStatusCode != 200) {
      System.out.println(new Date + " Error: the status code of url=" + url + " is " + result.getStatusCode)
    } else {
      HDFSUtil.uploadToHdfs(cacheFile, result.getContent.getBytes)
    }
  }
}
