package com

import com.ijunhai.common.logsystem.JunhaiLog
import org.bson.Document

/**
  * Created by admin on 2018/3/7.
  */
object Log2GpTest {
  def main(args: Array[String]): Unit = {
    val log = "NORMAL: [2018-03-07 12:04:38] CIP[180.150.188.136] DATA[{\"user\":{\"user_id\":\"7005171\",\"open_id\":\"\",\"user_name\":\"869536270\",\"gender\":\"男\",\"birth\":\"0000-00-00\",\"age\":\"\",\"phone_num\":\"\",\"email_num\":\"\",\"login_type\":\"\"},\"game\":{\"game_id\":\"200000146\",\"game_name\":\"山海经2\",\"game_ver\":\"1.8.1\"},\"device\":{\"screen_height\":\"540\",\"screen_width\":\"960\",\"device_id\":\"ffffffff-fe37-38a9-ffff-ffff90b2c5c2\",\"ios_idfa\":\"\",\"android_imei\":\"865166025475438\",\"android_adv_id\":\"\",\"android_id\":\"c8a37235aed986d2\",\"device_name\":\"GdSjn Zux375\",\"os_ver\":\"22\",\"sdk_ver\":\"4.2.0\",\"package_name\":\"com.junhai.shj2.dalan\",\"os_type\":\"android\",\"net_type\":\"wifi\",\"user_agent\":\"Mozilla/5.0 (Linux; Android 5.1.1; Zux375 Build/LMY48Z) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/39.0.0.0 Mobile Safari/537.36\"},\"channel_platform\":{\"ad_id\":\"WM_bd_shj2_ad_286_61\"},\"status\":\"success\",\"event\":\"login\",\"is_test\":\"regular\",\"data_ver\":\"1.0\",\"client_time_zone\":\"GMT+08:00\",\"client_ts\":1520395474,\"server_time_zone\":\"+08:00\",\"server_ts\":1520395478,\"client_ip\":\"60.166.95.25\"}]"
    val document: (Document, Boolean) = JunhaiLog.log2bson(log)
//print(document._1.toJson)



  }
}
