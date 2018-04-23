package com.kafka

import java.util

import com.ijunhai.process.agent.CoinProcess
import org.joda.time.DateTime

/**
  * Created by admin on 2018/2/24.
  */
object main {
  def main(args: Array[String]): Unit = {
    val str = "NORMAL: [2018-02-23 00:06:01] CIP[120.26.160.243] GAME_COIN_LOG[[{\"game_id\":20,\"channel_id\":\"102\",\"game_channel_id\":\"\",\"user_id\":\"4FA1173E46F34B31ABCCD460E8B43D7B\",\"role_id\":\"1910899\",\"role_name\":\"\\u661f\\u8fb0~\",\"server\":\"4652\",\"type\":4,\"trade_count\":-100,\"remain_count\":\"36396\",\"item_name\":\"\",\"item_count\":0,\"trade_desc\":\"\\u56e2\\u6218\\u526f\\u672c\\u5f00\\u5b9d\\u7bb1101\",\"trade_time\":\"1519315510\"},{\"game_id\":20,\"channel_id\":\"102\",\"game_channel_id\":\"\",\"user_id\":\"4FA1173E46F34B31ABCCD460E8B43D7B\",\"role_id\":\"1910899\",\"role_name\":\"\\u661f\\u8fb0~\",\"server\":\"4652\",\"type\":4,\"trade_count\":-100,\"remain_count\":\"36296\",\"item_name\":\"\",\"item_count\":0,\"trade_desc\":\"\\u56e2\\u6218\\u526f\\u672c\\u5f00\\u5b9d\\u7bb1101\",\"trade_time\":\"1519315512\"},{\"game_id\":20,\"channel_id\":\"102\",\"game_channel_id\":\"\",\"user_id\":\"4FA1173E46F34B31ABCCD460E8B43D7B\",\"role_id\":\"1910899\",\"role_name\":\"\\u661f\\u8fb0~\",\"server\":\"4652\",\"type\":4,\"trade_count\":178,\"remain_count\":\"36474\",\"item_name\":\"\",\"item_count\":0,\"trade_desc\":\"\\u819c\\u62dc\\u738b\\u8005\\u94bb\\u77f3\\u5956\\u52b1205\",\"trade_time\":\"1519315529\"}]]"
    val str2 = "NORMAL: [2018-02-23 00:05:53] CIP[118.89.53.188] GAME_COIN_LOG[{\t\"game_id\": \"SA1479182813\",\t\"channel_id\": \"2050\",\t\"game_channel_id\": \"2050\",\t\"user_id\": \"66040923ffd67bf5ee6348a536183457\",\t\"role_id\": \"6496000000255\",\t\"role_name\": \"贺曜遂\",\t\"server\": \"5496\",\t\"type\": \"4\",\t\"trade_count\": \"60\",\t\"remain_count\": \"166\",\t\"item_name\": \"钻石\",\t\"item_count\": \"0\",\t\"trade_time\": \"1519315310\",\t\"trade_desc\": \"\"}]"
    val str3 = "44\t6824\t0\t346613\t331000129\t秦逸权\t8332\t4\t-1\t120\t仙园摇钱树\t0\t仙园摇钱树\t1513527337"
//    println(CoinProcess.coinParser(str,"json")._1)
//    println(CoinProcess.coinParser(str,"json")._1.split(",,")(0))
//    println(CoinProcess.coinParser(str2,"json")._1.split(",,")(0))

    val a = (x: Int, y: Int) => x + y

 println(new DateTime().toLocalDateTime.toString("yyyyMMdd"))
  }
}
