package com.dao

import java.util.{Date, UUID}

import com.ijunhai.common.logsystem.{JunhaiLog, Monitor}
import com.ijunhai.common.{MD5Util, TimeUtil}
import com.ijunhai.storage.greenplum.GreenPlumSink
import org.bson.Document

/**
  * Created by admin on 2018/1/31.
  */
object ErrorMonitorTest {
  def main(args: Array[String]): Unit = {
    val gpSink = GreenPlumSink.apply("click_detail")

    gpSink.createAdvDetailTable("test")
//    val log = "NORMAL: [2018-01-13 23:57:16] CIP[52.212.89.218] AD_AF_LOG[{\"fb_adgroup_id\":null,\"click_time_selected_timezone\":null,\"download_time_selected_timezone\":\"2018-01-13 13:57:00.000+0000\",\"device_type\":\"iPhone 7 Plus\",\"download_time\":\"2018-01-13 13:57:00\",\"af_keywords\":null,\"attributed_touch_time\":null,\"click_time\":null,\"agency\":null,\"ip\":\"121.6.39.45\",\"cost_per_install\":null,\"fb_campaign_id\":null,\"is_retargeting\":false,\"app_name\":\"剑影奇缘 - 战斗到底 玄幻武侠\",\"re_targeting_conversion_type\":null,\"city\":\"Singapore\",\"af_sub1\":null,\"idfv\":\"51B25D1C-7D9F-4025-9984-C35901605A66\",\"af_sub2\":null,\"event_value\":null,\"cost_in_selected_currency\":null,\"af_sub3\":null,\"fb_adset_name\":null,\"af_sub4\":null,\"customer_user_id\":\"8DC5A349-3D30-4699-A360-645B964D1769\",\"mac\":null,\"af_sub5\":null,\"install_time_selected_timezone\":\"2018-01-13 15:56:41.791+0000\",\"campaign\":null,\"event_name\":null,\"event_time_selected_timezone\":\"2018-01-13 15:56:41.791+0000\",\"currency\":null,\"install_time\":\"2018-01-13 15:56:41\",\"fb_adgroup_name\":null,\"attributed_touch_type\":null,\"event_time\":\"2018-01-13 15:56:41\",\"platform\":\"ios\",\"sdk_version\":\"v4.4.1\",\"appsflyer_device_id\":\"1515880641140-9424879\",\"device_name\":\"Nikki’s iPhone7+\",\"selected_currency\":\"USD\",\"wifi\":true,\"media_source\":\"Organic\",\"country_code\":\"SG\",\"http_referrer\":null,\"idfa\":\"8DC5A349-3D30-4699-A360-645B964D1769\",\"fb_campaign_name\":null,\"bundle_id\":\"com.indie.wyfx.jt\",\"click_url\":null,\"language\":\"en-SG\",\"app_id\":\"id1192150887\",\"app_version\":\"1.1.0\",\"attribution_type\":\"organic\",\"af_siteid\":null,\"os_version\":\"10.3.2\",\"fb_adset_id\":null,\"event_type\":\"install\"}]"
//    val doc = JunhaiLog.log2bson(log)
//    val gpSink = GreenPlumSink.apply("monitor")
//    errorMonitor(gpSink,"haiwai","commonColumn may be null",doc._1.toJson())
  }

  def errorMonitor(GPSink: GreenPlumSink, source: String, info: String, data: String): Unit = {
    val document = new Document()
      .append(Monitor.source, source)
      .append(Monitor.info, info)
      .append(Monitor.data, data)
      .append(Monitor.time, TimeUtil.time2SqlDate(new Date().getTime, TimeUtil.MILLISECOND))
    GPSink.insert(document, Monitor.errorTable)
  }

}
