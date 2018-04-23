package com.ijunhai.batch

import com.ijunhai.batch.UpdateGP.tableNameMap
import org.bson.Document

/**
  * Created by admin on 2018/1/26.
  */
object test {
  def main(args: Array[String]): Unit = {
    val str = "{ \"user-user_id\" : \"6968379497\", \"user-user_name\" : " +
      "\"\", \"user-gender\" : \"\", \"user-birth\" : \"\", \"user-age\" : \"\", \"game-game_id\" :" +
      " \"119\", \"game-game_name\" : \"\", \"game-game_ver\" : \"\", \"game-company_id\" : \"2\"," +
      " \"agent-channel_id\" : \"8081\", \"agent-game_channel_id\" : \"60025\", \"agent-access_token\" :" +
      " \"\", \"device-screen_height\" : \"\", \"device-screen_width\" : \"\", \"device-device_id\" :" +
      " \"\", \"device-ios_idfa\" : \"\", \"device-android_imei\" : \"\", \"device-android_adv_id\" :" +
      " \"\", \"device-android_id\" : \"\", \"device-device_name\" : \"\", \"device-os_ver\" :" +
      " \"\", \"device-sdk_ver\" : \"\", \"device-package_name\" : \"\", \"device-os_type\" :" +
      " \"\", \"device-net_type\" : \"\", \"device-user_agent\" : \"\", \"event\" : \"login\", \"is_test\" :" +
      " \"regular\", \"data_ver\" : \"1.0\", \"client_time_zone\" : \"\", \"server_time_zone\" :" +
      " \"+08:00\", \"client_ts\" : 0, \"server_ts\" : 1521019970, \"client_ip\" : \"112.39.50.160\", \"country\" :" +
      " \"中国\", \"province\" : \"辽宁\", \"city\" : \"葫芦, \"first_order_date\" : \"\", \"server_date_day\" : \"2018-03-14\", \"server_date_hour\" : \"17\", \"server_date_minute\" : \"32\", \"reg_date\" : \"2017-07-10\" }"
    //    val a: String = tableNameMap.getOrElse("dalan","")
    val doc: Document = new Document().append("a","\"1521019970")
    println(doc.getString("a"))
    val str2 =
      if (doc.getString("a").contains("\"")) {
        println(doc.getString("a").replaceAll("\"", ""))
      }
println(doc.toJson())
    val d = Document.parse(doc.toJson())
    println(d.toJson())
  }
}
