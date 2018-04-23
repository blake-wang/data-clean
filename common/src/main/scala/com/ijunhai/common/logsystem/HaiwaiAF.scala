package com.ijunhai.common.logsystem

import com.ijunhai.common.{MD5Util, TimeUtil}
import org.bson.Document

/**
  * Created by Admin on 2018-01-31.
  */
class HaiwaiAF {
  var id=""
  var active_date =0
  var game =""
  var pf =0
  var click_time=TimeUtil.getDefaultSqlDate
  var install_time =TimeUtil.getDefaultSqlDate
  var agency=""
  var media_source=""
  var campaign=""
  var fb_campaign_name=""
  var fb_campaign_id=""
  var fb_adset_name =""
  var fb_adset_id=""
  var fb_adgroup_name=""
  var fb_adgroup_id =""
  var af_siteid =""
  var cost_per_install=""
  var country_code=""
  var city  =""
  var ip=""
  var wifi=0
  var language =""
  var appsflyer_device_id=""
  var customer_user_id=""
  var advertising_id=""
  var device_id=""
  var device_idfa1 =""
  var device_idfa2=""
  var mac=""
  var device_brand =""
  var device_type=""
  var os_version=""
  var sdk_version  =""
  var app_version=""
  var event_type =""
  var event_name=""
  var event_value =""
  var currency=""
  var event_time =TimeUtil.getDefaultSqlDate
  var af_sub1=""
  var af_sub2=""
  var af_sub3=""
  var af_sub4=""
  var af_sub5=""
  var click_url =""
  var attribution_type=""
  var http_referrer=""
  var platform=""
  var android_id=""
  var imei=""
  var device_model=""
  var idfa=""
  var idfv=""
  var device_name=""
  val app_name=""
  var game_id=""
  var time=""
  var event_date=0
  override def toString: String ={
    appsflyer_device_id+game+pf+time
  }
}

object HaiwaiAF {
  val id="id"
  val game_id="game_id"
  val active_date ="active_date"
  val game ="game" //
  val pf ="pf"
  val click_time="click_time"
  val install_time="install_time"
  val agency="agency"
  val media_source="media_source"
  val campaign="campaign"
  val fb_campaign_name="fb_campaign_name"
  val fb_campaign_id="fb_campaign_id"
  val fb_adset_name ="fb_adset_name"
  val fb_adset_id="fb_adset_id"
  val fb_adgroup_name="fb_adgroup_name"
  val fb_adgroup_id ="fb_adgroup_id"
  val af_siteid ="af_siteid"
  val cost_per_install="cost_per_install"
  val country_code="country_code"
  val city ="city"
  val ip="ip"
  val wifi="wifi"
  val language ="language"
  val appsflyer_device_id="appsflyer_device_id"
  val customer_user_id="customer_user_id"
  val advertising_id="advertising_id"
  val device_id="device_id"
  val device_idfa1 ="device_idfa1" //
  val device_idfa2="device_idfa2"
  val mac="mac"
  val device_brand ="device_brand"
  val device_type="device_type"  //
  val os_version="os_version"
  val sdk_version  ="sdk_version" //
  val app_version="app_version"
  val event_type ="event_type"
  val event_name="event_name"
  val event_value ="event_value"
  val currency="currency"
  val event_time ="event_time"
  val af_sub1="af_sub1"
  val af_sub2="af_sub2"
  val af_sub3="af_sub3"
  val af_sub4="af_sub4"
  val af_sub5="af_sub5"
  val click_url ="click_url"
  val attribution_type="attribution_type"
  val http_referrer="http_referrer"
  val platform="platform"
  val android_id="android_id"
  val imei="imei"
  val device_model="device_model"
  val idfa="idfv"
  val idfv="idfv"
  val device_name="device_name"
  val app_name="app_name"
  val bundle_id="bundle_id"
  val time="time"
  val event_date="event_date"

//  id                  | character varying(50)       | not null
//  active_date         | integer                     | not null default 19700701
//  game                | character varying(50)       | not null default ''::character varying
//  pf                  | smallint                    | not null default 0::smallint
//    click_time          | timestamp without time zone | not null default '1970-07-01 00:00:00'::timestamp without time zone
//  install_time        | timestamp without time zone | not null default '1970-07-01 00:00:00'::timestamp without time zone
//  agency              | character varying(50)       | not null default ''::character varying
//  media_source        | character varying(50)       | not null default ''::character varying
//  campaign            | character varying(50)       | not null default ''::character varying
//  fb_campaign_name    | character varying(50)       | not null default ''::character varying
//  fb_campaign_id      | character varying(50)       | not null default ''::character varying
//  fb_adset_name       | character varying(50)       | not null default ''::character varying
//  fb_adset_id         | character varying(50)       | not null default ''::character varying
//  fb_adgroup_name     | character varying(50)       | not null default ''::character varying
//  fb_adgroup_id       | character varying(50)       | not null default ''::character varying
//  af_siteid           | character varying(50)       | not null default ''::character varying
//  cost_per_install    | character varying(50)       | not null default ''::character varying
//  country_code        | character varying(10)       | not null default ''::character varying
//  city                | character varying(50)       | not null default ''::character varying
//  ip                  | character varying(20)       | not null default ''::character varying
//  wifi                | smallint                    | not null default 0::smallint
//    language            | character varying(10)       | not null default ''::character varying
//  appsflyer_device_id | character varying(50)       | not null default ''::character varying
//  customer_user_id    | character varying(100)      | not null default ''::character varying
//  advertising_id      | character varying(50)       | not null default ''::character varying
//  device_id           | character varying(50)       | not null default ''::character varying
//  device_idfa1        | character varying(50)       | default ''::character varying
//  device_idfa2        | character varying(50)       | default ''::character varying
//  mac                 | character varying(50)       | not null default ''::character varying
//  device_brand        | character varying(20)       | not null default ''::character varying
//  device_type         | character varying(20)       | not null default ''::character varying
//  os_version          | character varying(20)       | not null default ''::character varying
//  sdk_version         | character varying(20)       | not null default ''::character varying
//  app_version         | character varying(20)       | not null default ''::character varying
//  event_type          | character varying(20)       | not null default ''::character varying
//  event_name          | character varying(20)       | not null default ''::character varying
//  event_value         | character varying(50)       | not null default ''::character varying
//  currency            | character varying(10)       | not null default ''::character varying
//  event_time          | timestamp without time zone | not null default '1970-07-01 00:00:00'::timestamp without time zone
//  af_sub1             | character varying(20)       | not null default ''::character varying
//  af_sub2             | character varying(20)       | not null default ''::character varying
//  af_sub3             | character varying(20)       | not null default ''::character varying
//  af_sub4             | character varying(20)       | not null default ''::character varying
//  af_sub5             | character varying(20)       | not null default ''::character varying
//  click_url           | character varying(50)       | not null default ''::character varying
//  attribution_type    | character varying(20)       | not null default ''::character varying
//  http_referrer       | character varying(50)       | not null default ''::character varying
def load(document: Document): HaiwaiAF ={
  val haiwaiAf=new HaiwaiAF
   haiwaiAf.event_time =if (JunhaiLog.getString(document,HaiwaiAF.event_time)=="") TimeUtil.getDefaultSqlDate else TimeUtil.dataString2SqlDate("yyyy-MM-dd HH:mm:ss",JunhaiLog.getString(document,HaiwaiAF.event_time))
   haiwaiAf.event_date=if(haiwaiAf.event_time!=TimeUtil.getDefaultSqlDate) TimeUtil.time2DateString("yyyyMMdd",TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss",JunhaiLog.getString(document,HaiwaiAF.install_time),TimeUtil.SECOND),TimeUtil.SECOND).toInt else 19700701
   haiwaiAf.active_date=TimeUtil.time2DateString("yyyyMMdd",TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss",JunhaiLog.getString(document,HaiwaiAF.install_time),TimeUtil.SECOND),TimeUtil.SECOND).toInt
   haiwaiAf.game=JunhaiLog.getString(document,HaiwaiAF.bundle_id)
   haiwaiAf.game_id=JunhaiLog.getString(document,HaiwaiAF.game_id)
   haiwaiAf.time=JunhaiLog.getString(document,HaiwaiAF.event_time)
   haiwaiAf.platform=JunhaiLog.getString(document,HaiwaiAF.platform)
   if(haiwaiAf.platform=="android"){
     haiwaiAf.pf=0
     haiwaiAf.device_id=JunhaiLog.getString(document,HaiwaiAF.customer_user_id)
     haiwaiAf.device_idfa1=JunhaiLog.getString(document,HaiwaiAF.android_id)
     haiwaiAf.device_idfa2=JunhaiLog.getString(document,HaiwaiAF.imei)
     haiwaiAf.device_brand=JunhaiLog.getString(document,HaiwaiAF.device_brand)
     haiwaiAf.device_type=JunhaiLog.getString(document,HaiwaiAF.device_model)
   }else if(haiwaiAf.platform=="ios"){
     haiwaiAf.pf=1
     haiwaiAf.device_id=JunhaiLog.getString(document,HaiwaiAF.customer_user_id)
     haiwaiAf.device_idfa1=JunhaiLog.getString(document,HaiwaiAF.idfa)
     haiwaiAf.device_idfa2=JunhaiLog.getString(document,HaiwaiAF.idfv)
     haiwaiAf.device_brand=JunhaiLog.getString(document,HaiwaiAF.device_type)
     haiwaiAf.device_type=JunhaiLog.getString(document,HaiwaiAF.device_model)
   }
   haiwaiAf.click_time=if (JunhaiLog.getString(document,HaiwaiAF.click_time)=="") TimeUtil.getDefaultSqlDate else TimeUtil.dataString2SqlDate("yyyy-MM-dd HH:mm:ss",JunhaiLog.getString(document,HaiwaiAF.click_time))
   haiwaiAf.install_time=TimeUtil.dataString2SqlDate("yyyy-MM-dd HH:mm:ss",JunhaiLog.getString(document,HaiwaiAF.install_time))
   haiwaiAf.agency=JunhaiLog.getString(document,HaiwaiAF.agency)
   haiwaiAf.media_source=JunhaiLog.getString(document,HaiwaiAF.media_source)
   haiwaiAf.campaign=JunhaiLog.getString(document,HaiwaiAF.campaign )
   haiwaiAf.fb_campaign_name=JunhaiLog.getString(document,HaiwaiAF.fb_campaign_name )
   haiwaiAf.fb_campaign_id=JunhaiLog.getString(document,HaiwaiAF.fb_campaign_id)
   haiwaiAf.fb_adset_name=JunhaiLog.getString(document,HaiwaiAF.fb_adset_name )
   haiwaiAf.fb_adset_id=JunhaiLog.getString(document,HaiwaiAF.fb_adset_id)
   haiwaiAf.fb_adgroup_name=JunhaiLog.getString(document,HaiwaiAF.fb_adgroup_name)
   haiwaiAf.fb_adgroup_id =JunhaiLog.getString(document,HaiwaiAF.fb_adgroup_id)
   haiwaiAf.af_siteid =JunhaiLog.getString(document,HaiwaiAF.af_siteid)
   haiwaiAf.cost_per_install=JunhaiLog.getString(document,HaiwaiAF.cost_per_install)
   haiwaiAf.country_code=JunhaiLog.getString(document,HaiwaiAF.country_code)
   haiwaiAf.city  =JunhaiLog.getString(document,HaiwaiAF.city)
   haiwaiAf.ip=JunhaiLog.getString(document,HaiwaiAF.ip)
   haiwaiAf.wifi=if(JunhaiLog.getBoolean(document,HaiwaiAF.wifi)) 1 else 0
   haiwaiAf.language =JunhaiLog.getString(document,HaiwaiAF.language)
   haiwaiAf.appsflyer_device_id=JunhaiLog.getString(document,HaiwaiAF.appsflyer_device_id)
   haiwaiAf.customer_user_id=JunhaiLog.getString(document,HaiwaiAF.customer_user_id)
   haiwaiAf.advertising_id=JunhaiLog.getString(document,HaiwaiAF.advertising_id)
   haiwaiAf.mac=JunhaiLog.getString(document,HaiwaiAF.mac)
   haiwaiAf.os_version=JunhaiLog.getString(document,HaiwaiAF.os_version)
   haiwaiAf.sdk_version  =JunhaiLog.getString(document,HaiwaiAF.sdk_version) //
   haiwaiAf.app_version=JunhaiLog.getString(document,HaiwaiAF.app_version)
   haiwaiAf.event_type =JunhaiLog.getString(document,HaiwaiAF.event_type)
   haiwaiAf.event_name=JunhaiLog.getString(document,HaiwaiAF.event_name)
   haiwaiAf.event_value =JunhaiLog.getString(document,HaiwaiAF.event_value)
   haiwaiAf.currency=JunhaiLog.getString(document,HaiwaiAF.currency)

   haiwaiAf.af_sub1=JunhaiLog.getString(document,HaiwaiAF.af_sub1)
   haiwaiAf.af_sub2=JunhaiLog.getString(document,HaiwaiAF.af_sub2)
   haiwaiAf.af_sub3=JunhaiLog.getString(document,HaiwaiAF.af_sub3)
   haiwaiAf.af_sub4=JunhaiLog.getString(document,HaiwaiAF.af_sub4)
   haiwaiAf.af_sub5=JunhaiLog.getString(document,HaiwaiAF.af_sub5)
   haiwaiAf.click_url =if(JunhaiLog.getString(document,HaiwaiAF.click_url).length>=512) JunhaiLog.getString(document,HaiwaiAF.click_url).substring(0,500) else JunhaiLog.getString(document,HaiwaiAF.click_url)
   haiwaiAf.attribution_type=JunhaiLog.getString(document,HaiwaiAF.attribution_type)
   haiwaiAf.http_referrer=if(JunhaiLog.getString(document,HaiwaiAF.http_referrer).length>=512) JunhaiLog.getString(document,HaiwaiAF.http_referrer).substring(0,500) else JunhaiLog.getString(document,HaiwaiAF.http_referrer)
   haiwaiAf.id=MD5Util.getMD5(haiwaiAf.toString())

   haiwaiAf
}
  def unload(haiwaiAF: HaiwaiAF): Document = {
    val document = new Document()
    document.put(event_date,haiwaiAF.event_date)
    document.put(id,haiwaiAF.id)
    document.put(active_date,haiwaiAF.active_date)
    document.put(game,haiwaiAF.game)
    document.put(game_id,haiwaiAF.game_id)
    document.put(pf,haiwaiAF.pf)
    document.put(click_time,haiwaiAF.click_time)
    document.put(install_time,haiwaiAF.install_time)
    document.put(agency,haiwaiAF.agency)
    document.put(media_source,haiwaiAF.media_source)
    document.put(campaign,haiwaiAF.campaign)
    document.put(fb_campaign_name,haiwaiAF.fb_campaign_name)
    document.put(fb_campaign_id,haiwaiAF.fb_campaign_id)
    document.put(fb_adset_name,haiwaiAF.fb_adset_name)
    document.put(fb_adset_id,haiwaiAF.fb_adset_id)
    document.put(fb_adgroup_name,haiwaiAF.fb_adgroup_name)
    document.put(fb_adgroup_id,haiwaiAF.fb_adgroup_id)
    document.put(af_siteid,haiwaiAF.af_siteid)
    document.put(cost_per_install,haiwaiAF.cost_per_install)
    document.put(country_code,haiwaiAF.country_code)
    document.put(city,haiwaiAF.city)
    document.put(ip,haiwaiAF.ip)
    document.put(wifi,haiwaiAF.wifi)
    document.put(language,haiwaiAF.language)
    document.put(appsflyer_device_id,haiwaiAF.appsflyer_device_id)
    document.put(customer_user_id,haiwaiAF.customer_user_id)
    document.put(advertising_id,haiwaiAF.advertising_id)
    document.put(device_id,haiwaiAF.device_id)
    document.put(device_idfa1,haiwaiAF.device_idfa1)
    document.put(device_idfa2,haiwaiAF.device_idfa2)
    document.put(mac,haiwaiAF.mac)
    document.put(device_brand,haiwaiAF.device_brand)
    document.put(device_type,haiwaiAF.device_type)
    document.put(os_version,haiwaiAF.os_version)
    document.put(sdk_version,haiwaiAF.sdk_version)
    document.put(app_version,haiwaiAF.app_version)
    document.put(event_type,haiwaiAF.event_type)
    document.put(event_name,haiwaiAF.event_name)
    document.put(event_value,haiwaiAF.event_value)
    document.put(currency,haiwaiAF.currency)
    document.put(event_time,haiwaiAF.event_time)
    document.put(af_sub1,haiwaiAF.af_sub1)
    document.put(af_sub2,haiwaiAF.af_sub2)
    document.put(af_sub3,haiwaiAF.af_sub3)
    document.put(af_sub4,haiwaiAF.af_sub4)
    document.put(af_sub5,haiwaiAF.af_sub5)
    document.put(click_url,haiwaiAF.click_url)
    document.put(attribution_type,haiwaiAF.attribution_type)
    document.put(http_referrer,haiwaiAF.http_referrer)


    document
  }
}
