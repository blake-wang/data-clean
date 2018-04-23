package com.ijunhai;

/**
  * Created by Admin on 2017-07-31.
  */
object HelloSpark {
  val rows = "rows"
  val table = "table"
  val type_ ="type"
  val data = "data"
  //  var instance: RedisClient = null
  //  val REDIS_TIMEOUT: Int = 3000



  def main(args: Array[String]) {

//    println(MD5Util.getMD5("123456"))



//    val jedis=RedisClient.getInstatnce.getJedis
//    jedis.get("agent_10121_3787403")
//    val ajedis=RedisClientAlone.getInstatnce.getJedis
//    ajedis.get("")

//    println(TimeUtil.getDefaultSqlDate)
//    val sparkConf=new SparkConf().setMaster("local").setAppName("hello_spark")
//    val sc=new SparkContext(sparkConf)

//    val rdd1=sc.textFile("file:///C:\\Users\\Admin\\Desktop\\data\\order_psql_2017-11-20.txt")
////    val rdd=sc.textFile("file:///C:\\Users\\Admin\\Desktop\\data\\alipay\\order_sn_money_09.csv")
//    val rdd2=sc.textFile("file:///C:\\Users\\Admin\\Desktop\\data\\order_2017-11-20.csv")
////    println(rdd1.count())
//    val result=rdd1.map(line=>(line,1)).reduceByKey(_+_).filter(_._2>1)
//    println(result.count())
//    result.foreach(println)

//    val set=rdd1.collect().toSet
//    val order_sn=rdd2.filter(line=> !set.contains(line))
//    println(order_sn.count())
//    order_sn.foreach(println)

//    val array=Array(1,2,2,4,5,3,4,19,1,1,20,1,1,5,1,2,21,23,4,7,1,28,17,3,1,27,3,10,1,22,26,6,5,11,4,3,22,24,11,3,4,19,3,2,2,14,2)
//    println(array.sum)

//    val document=new Document().append("date",new java.sql.Date(1510735240000L))
//    TimeUtil.time2SqlDate(1510817226L,TimeUtil.SECOND)
//    println(JunhaiLog.documentClean2Json(document).toJson())

//    val user="NORMAL: [2017-11-16 13:12:36] CIP[36.96.157.244] AD_CLICK[{\"junhai_adid\":\"15800\",\"game\":\"U200000048\",\"click_time\":\"2017-11-16 13:12:36\"}]"
//    val sparkConf=new SparkConf().setMaster("local").setAppName("hello_spark")
//    val sc=new SparkContext(sparkConf)
//    val array=(0 until(1000)).map(line=> user)
//    val rDD=sc.parallelize(array)
//    val rdd=AdvClickProcess.advLogAnalysis(rDD)
//    val clickDetail= AdvClickProcess.getDetail(rdd.map(_._1))
//    val start=System.currentTimeMillis()
//    val clickDetailResult=Save2Redis.saveKeyValueAlone(clickDetail,3*24*60*60)//详细数据放redis去重
//    println(clickDetailResult.count())
//    println(System.currentTimeMillis()-start)

//    val array=Set("a","b","c","d")
//    val array1=Set("a","b","c")
//    println(array==array1)

//    System.setProperty("hadoop.home.dir", "E:\\64bit_software\\64bit_software\\hadoop-2.6.0-x64\\hadoop-2.6.0")
//    val str="{\"binlogFilename\":\"mysql-bin.000074\",\"xid\":45817888,\"nextPosition\":56973378,\"rows\":[{\"database\":\"youyun_account\",\"data\":{\"server\":691,\"type\":1,\"user_voucher_info_id\":4294967296,\"pay_status\":1,\"update_time\":1510735230000,\"jh_channel\":\"WM_bd_sbkryol_ad_177_34\",\"cp_trade_sn\":\"2017111556940460268\",\"sdk_version\":\"4.0\",\"pay_type\":\"1\",\"id\":9399421,\"game_id\":100000209,\"application_version\":\"101\",\"create_ip\":\"218.56.162.96\",\"source_from\":1,\"reduce_money\":0.00,\"goods_name\":\"2017111556940460268\",\"game_role_name\":\"翁从雪\",\"device_id\":\"00000000-0277-23c1-be51-b1da0033c587\",\"exchange_rate\":0,\"create_time\":\"2017-11-15 16:40:30\",\"goods_id\":\"2017111556940460268\",\"payment_trade_sn\":\"2017111521001104460502607109\",\"game_role_id\":\"1200691\",\"game_url\":\"http://agent.ijunhai.com/pay/payFinish/game_channel_id/101232/game_id/72\",\"money\":10.00,\"user_id\":4101891,\"imei\":\"867516026060100\",\"order_sn\":2017111524603271687,\"status\":4},\"type\":\"UPDATE\",\"table\":\"order\"}]}"
//    val user="{\"binlogFilename\":\"mysql-bin.000074\",\"xid\":45817870,\"nextPosition\":56968891,\"rows\":[{\"database\":\"youyun_account\",\"data\":{\"device_id\":\"04C3645B-474A-402A-94D3-05F987368405\",\"user_name\":\"pqqmzz\",\"system_name\":\"\",\"sex\":1,\"icon\":\"\",\"tel_num\":\"\",\"birth\":-62170185600000,\"reg_type\":3,\"register_time\":1510735248,\"password\":\"72c7c08028b62b630f0f2f2a1e4a84083470f39858e0005a8e46538cd76aab77\",\"jh_channel\":\"appstore\",\"user_id\":4686004,\"tel_status\":0,\"system_version\":\"11.1.1\",\"nick_name\":\"\",\"imei\":\"8656BDB4-6D98-4592-A5B5-B8AD43CB7FF8\",\"register_ip\":\"115.200.215.59\",\"password_str\":\"mAZ8)<\",\"email\":\"\",\"jh_app_id\":\"200000106\",\"status\":1},\"type\":\"INSERT\",\"table\":\"user\"}]}"
//    val document=Document.parse(user)
//    val sparkConf=new SparkConf().setMaster("local").setAppName("hello_spark")
//    val sc=new SparkContext(sparkConf)
//    val array=Array(document)
//    val rdd=sc.parallelize(array).flatMap(line => {
//      line.get(rows).asInstanceOf[util.ArrayList[Document]].map(doc => {
//        val tableName=JunhaiLog.getString(doc,table)
//        val typeAction =JunhaiLog.getString(doc,type_)
//        (doc.get(data).asInstanceOf[Document],tableName,typeAction)
//      })
//    }).map(line=>{
//      DalanUser.unload(DalanUser.load(line._1))
//    })
//    println(Save2GreenPlum.saveDStream(rdd,"youyun_log","dl_user").count())
//    GreenPlumManager.query("select * from dl_user","youyun_log")


//    println(new DalanActive)
//    println(TimeUtil.dateString2Time("yyyy-MM-dd HH:mm","2017-11-13 12:02",TimeUtil.SECOND))

//    val database="youyun_log"
//    val conn=GreenPlumManager.getConnect(database)
//    val sql="insert into temp(id,time,data) values(?,\'2017-11-16 23:31:11\',?);"
//    val pstmt=conn.prepareStatement(sql)
//    val date=new java.sql.Date(1510817226000L)
//    println(date.getTime)
//    println(date.toString)
//    pstmt.setInt(1,1)
//    pstmt.setString(2,"data")
//    pstmt.executeUpdate()

//    val string="NORMAL: [2017-11-06 10:12:22] CIP[115.239.212.194] AD_CLICK[{\"junhai_adid\":\"13256\",\"game\":\"U200000048\",\"click_time\":\"2017-11-06 10:12:22\"}]"
//    val array=AdvClickProcess.getAdvContent(string)
//    println(array._1)
//    println(array._2)
//    println(array._3)

//    (0 until(30)).foreach(line=>{
//      val redisConn = RedisClientAlone.getInstatnce.getJedis
//      redisConn.set("hh","hell")
//      println(redisConn.get("hh"))
//    })
//    val uuid=UUID.randomUUID().toString
//
//    println(uuid)
//    println(Save2Redis.isRunning("hello",uuid,10))


//    val acc=sc.longAccumulator
//    val acc1=sc.longAccumulator
//
////    val rdd=sc.parallelize(array).map(line=>{
////      acc.add(1)
////      (new Document(),line)
////    })
//    val rdd1=sc.parallelize(array).map(line=>{
//      acc.add(1)
//      (new Document(),line)
//    }).filter(line=>{
//      acc1.add(1)
//      line._2!=""
//    }).cache()
//
//    println(acc.value)
////    println(rdd.count())
//    println(acc.value)
////    println(rdd.count())
//    println(acc.value)
//
//    println(acc1.value)
//    println(rdd1.count())
//    println(acc1.value)
//    println(rdd1.count())
//    println(acc1.value)
//
//    rdd1.persist()

//    Save2Redis.distinctByWindow(rdd).foreach(println)


//    val ip="192."
//      println(ip.split('.').length==4)
//    val str="2016-06-30 16:17:28"
//    println(JunhaiLog.isNumber(str))
//    val document=Document.parse("{sdf:23}")
//    println(document.get("sdf"))

    //  System.setProperty("hadoop.home.dir", "E:\\64bit_software\\64bit_software\\hadoop-2.6.0-x64\\hadoop-2.6.0")
    //  val sparkConf = new SparkConf().setMaster("local").setAppName("hello_spark")
    //  val ssc = new JavaStreamingContext(sparkConf, Seconds(1))
    //  ssc.start()
    //  ssc.awaitTermination()


    //    val Str="{hello:123,update_time:1506404891,clentIp:\"a\"}"
    //    val doc=Document.parse(Str)
    //    println(doc)
    //    println(doc.toJson())
    //    val json=doc.toJson()
    //    val flagStr="update_time\" : "
    //    val pos=json.indexOf(flagStr)+flagStr.length+10
    //    println(json.substring(0,pos)+"000"+json.substring(pos))

    //  val junhaiLog=new JunhaiLog
    //  println(junhaiLog.agent)
    //  junhaiLog.agent="agent"
    //  println(junhaiLog.agent)

    //  val oldStr="你好"
    //  val newStr = new String(oldStr.getBytes(), "UNICODE")
    //  val str=new String(newStr.getBytes(),"UTF-8")
    //  println(str)

    //     val REDIS_TIMEOUT: Int = 3000
    //     val config: JedisPoolConfig = new JedisPoolConfig
    //    val host="192.168.1.110"
    //    val port=6379
    ////    val redisPool=RedisClient.getPool.getResource
    //    config.setMaxTotal(PropertiesUtils.getInt(REDIS_MaxTotal, DEFAULT_MAX_TOTAL))
    //    // set max idle jedis instance amount, default 8
    //    config.setMaxIdle(PropertiesUtils.getInt(REDIS_MaxIdle, DEFAULT_MAX_IDLE))
    //    // set min idle jedis instance amount, default 0
    //    config.setMinIdle(PropertiesUtils.getInt(REDIS_MinIdle, DEFAULT_MIN_IDLE))
    //    // max wait time when borrow a jedis instance
    //    config.setMaxWaitMillis(PropertiesUtils.getLong(REDIS_MaxWaitMillis, DEFAULT_MAX_WAIT_MILLIS))
    //    config.setTestOnBorrow(true)
    //    val pool = new JedisPool(config, host, port, REDIS_TIMEOUT).getResource
    //    val jedis=new Jedis(host,port)
    //    val redisConn=RedisClient.getInstatnce.getJedis
    //    val start=System.currentTimeMillis()
    //    for (i<-1 until 10000){
    //      redisConn.hset("agent_171_17295313","order_server_ts","1505463635")
    ////      redisConn.hmget("agent_171_17295313","order_server_ts")
    ////      redisPool.hmget("agent_171_17295313","order_server_ts")
    ////      redisConn.hmget("agent_171_17295313","order_server_ts")
    ////    redisConn.hgetAll("agent_171_17295313")
    //    }
    //    val end=System.currentTimeMillis()
    //    println(end-start)

    //    val redisConn = RedisClient.getInstatnce.getJedis
    //    println(TimeUtil.time2DateString("HH",1505801177,TimeUtil.SECOND))
    //    println(TimeUtil.time2DateString("mm",1505801177,TimeUtil.SECOND))


    //    Save2Kafka.saveMetrics(new Date(),HelloSpark)

    //    GetTables.test="123"
    //    println(GetTables.test)
    //    GetTables.print()

    //    val str="{binlogFilename:\"你好\",\"xid\":40,\"nextPosition\":998,\"rows\":{\"database\":\"youyun_agent\",\"data\":{\"channel_id\":199,\"order_sn\":2017032431224822135,\"status\":4,\"game_id\":1,\"create_ip\":\"39.128.23.215\"},\"type\":\"UPDATE\",\"table\":\"agent_order_copy\"}}"
    //    val doc=Document.parse(str)
    //
    //    println(addColumn(doc).append("test","test"))
    //    println()
    //    println(TimeUtil.dateString2Time("yyyy-MM-dd hh:mm:ss","2014-11-13 04:16:57",TimeUtil.SECOND))
    //    println(Resources.getResource("17monipdb.dat").toString)

    //    val config=XMLUtil.xml2bson("d:/testFile/cleanConfig.xml")
    //    println(config.get("config").asInstanceOf[Document].get("order").asInstanceOf[Document].get("order").asInstanceOf[Document].getString("order_sn"))
    //    "hello".asInstanceOf[Int]

    //    val regex="([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}"
    //    val ip="100.18.251.124"
    //    println(ip.matches(regex))

//      System.setProperty("hadoop.home.dir", "E:\\64bit_software\\64bit_software\\hadoop-2.6.0-x64\\hadoop-2.6.0")
//      val sparkConf=new SparkConf().setMaster("local").setAppName("hello_spark")
//      val sc=new SparkContext(sparkConf)
//      val rdd1=sc.textFile("file:///C:\\Users\\Admin\\Desktop\\data\\alipay\\order_sn_money_09.csv")
//      val rdd2=sc.textFile("file:///C:\\Users\\Admin\\Desktop\\data\\alipay\\orderjh_sn_money_09_1.csv")
//      val rdd3=sc.textFile("file:///C:\\Users\\Admin\\Desktop\\data\\alipay\\order_sn_money_09jh.csv")
//      val rdd4=sc.textFile("file:///C:\\Users\\Admin\\Desktop\\data\\alipay\\orderjh_sn_money_09jh.csv")
//      val zhifubao09=sc.textFile("file:///C:\\Users\\Admin\\Desktop\\data\\alipay\\zhifubao09.csv")
//      val weixin09=sc.textFile("file:///C:\\Users\\Admin\\Desktop\\data\\alipay\\weixin09.csv")

//      val zhifubao_order=zhifubao09.map(line=>{
//        line.split(",")
//      }).filter(_.length==12)
//        .map(line=>{
//        (line(2).substring(1,line(2).substring(2).length-2),line(6))
//      }).take(10).foreach(println)

//    val weixin_order=weixin09.map(line=>{
//      line.split("\",\"")
//    }).filter(_.length==21).map(line=>{
//      (line(1),line(17).toDouble)
//    })
////      val rdd6=sc.textFile("file:///C:\\Users\\Admin\\Desktop\\data\\alipay\\weixin.txt").map(_.toDouble).sum()
////    println(rdd6)
//      val zhifubao=sc.textFile("file:///C:\\Users\\Admin\\Desktop\\data\\alipay\\09.txt").distinct().filter(_!="").map(line=>{
//        val array=line.split(' ')
//        (array(0),array(1))
//      })
//      val weixin=sc.textFile("file:///C:\\Users\\Admin\\Desktop\\data\\alipay\\09_wx.txt").distinct().filter(_!="").map(line=>{
//        val array=line.split(' ')
//        (array(0),array(1))
//      })
//
//      val rdd5=rdd1.union(rdd2).union(rdd3).union(rdd4).map(line=>{
//        val array=line.split(' ')
//        (array(0),array(1).toDouble)
//      }).collectAsMap()
//    //      val rdd2=sc.textFile("file:///C:\\Users\\Admin\\Desktop\\data/result_2.csv")
//      val databaseOrder=rdd1.map(line=>{
//        val array=line.split(' ')
//        (array(0),array(1))
//      }).collectAsMap()
//      val databaseOrder2=rdd2.map(line=>{
//        val array=line.split(' ')
//        (array(0),array(1))
//      }).collectAsMap()
//      val databaseOrder3=rdd3.map(line=>{
//        val array=line.split(' ')
//        (array(0),array(1))
//      }).collectAsMap()
//
//      val databaseOrder4=rdd4.map(line=>{
//        val array=line.split(' ')
//        (array(0),array(1))
//      }).collectAsMap()
//      println(zhifubao.filter(databaseOrder.contains(_)).count()) //146275
//      println(zhifubao.filter(databaseOrder2.contains(_)).count()) //22366
//
//      println(zhifubao.filter(databaseOrder3.contains(_)).count()) //24381 0
//      println(weixin.filter(databaseOrder4.contains(_)).count())  //148290  1940 8194
//      println(zhifubao.filter(line=> !databaseOrder2.contains(line) && !databaseOrder.contains(line) && !databaseOrder3.contains(line) && !databaseOrder4.contains(line))
//    val error_zhifubao=zhifubao_order.map(line=>{
//      (line._1,rdd5.getOrElse(line._1,0.0),line._2)
//    }).filter(_._2!=0.0)
//
//    println(error_zhifubao.count())
//    error_zhifubao.take(10).foreach(println)
//    weixin.filter(line=> !rdd5.contains(line._1)).foreach(println)
//    println(zhifubao.filter(line=> !rdd5.contains(line._1))
//      //      .count()
//      .map(line=>{
//      rdd5.getOrElse(line._1,0.0)
//      line._2
//    })//-15593784.24
//    ) //2015  75
//
//    println(weixin.filter(line=> rdd5.contains(line))
//      //      .count()
//      .map(rdd5.getOrElse(_,0.0)).sum()-38971244.86
//    )
//    weixin.filter(line=> !rdd5.contains(line))
//      //      .count()
//      .map(rdd5.getOrElse(_,"0")).foreach(println)
//
//      zhifubao.filter(line=> !databaseOrder2.contains(line) && !databaseOrder.contains(line)).foreach(println)
//
//      println(weixin.filter(databaseOrder.contains(_)).count()) //532752
//      println(weixin.filter(databaseOrder2.contains(_)).count()) //97488
//      println(weixin.filter(!databaseOrder.contains(_)).count()) //105741
//      println(weixin.filter(!databaseOrder2.contains(_)).count())  //541005
//      println(weixin.filter(line=> !databaseOrder2.contains(line) && !databaseOrder.contains(line)).count()) //8253
//      weixin.filter(line=> !databaseOrder2.contains(line) && !databaseOrder.contains(line)).foreach(println)

    //      println(rdd1.distinct().count())

    //    val string="{a:1,b:{c:2},d:{e:{f:3}},p:{g:[{a:1} {b:2}],o:1},h:[1 2 3],j:[\"1\" \"2\"]}"
    //    val CONTENT="{user:{user_id:\"123\",user_name:\"风\",gender:\"男\",birth:\"1990-07\",age:\"27\"},agent:{channel_id:\"10086\",game_channel_id:\"12306\"},game:{game_id:\"127\",game_name:\"六界飞仙\",game_ver:\"1.0.0\"},device:{screen_height:\"1280\",screen_width:\"920\",device_id:\"854159154142445\",ios_idfa:\"867876026287362\",android_imei:\"55da5367-fa8e-4013-9730-e716395c845c\",android_adv_id:\"867876026287362\",android_id:\"867876026287362\",device_name:\"Xiaomi MI 5C\",os_ver:\"6.0\",sdk_ver:\"2.1\",package_name:\"com.junhai.qyj.bingniao\",os_type:\"android\",net_type:\"wifi\",user_agent:\"Dalvik/2.1.0 (Linux; U; Android 6.0.1; OPPO R9s Build/MMB29M)\"},role:{role_level:\"55\",role_name:\"逍遥•碧薇\",role_server:\"1051\",role_id:\"1050000428\",role_type:\"法师\",role_gender:\"女\"},order:{order_sn:\"2017070469555350379\",cp_trade_sn:\"2017070469555350379\",channel_trade_sn:\"2017070469555350379\",request_url:\"http://a2017070469555350379\",http_code:\"200\",request_result:{\"ret\":0},order_step:\"create_order\",cny_amount:50.0 ,currency_amount:10.0 ,currency_type:\"rmb\",usd_amount:7.2 ,order_status:\"success\",order_type:\"alipay\"},event:\"order\",is_test:\"test\",data_ver:\"1.0\",client_time_zone:\"+08:00\",client_ts:1499616000,server_time_zone:\"+08:00\",server_ts:1499616000,client_ip:\"14.23.56.194\"}"
    //  println(DataProcess.documentFlat("",Document.parse(CONTENT)))
    //    System.setProperty("hadoop.home.dir", PropertiesUtils.get(RocketMQConstants.HADOOP_DIR))
    //    val sparkConf=new SparkConf().setMaster("local").setAppName("hello_spark")
    //    val sc=new SparkContext(sparkConf)
    //    val rdd=sc.parallelize(Array("{}","{}","{}"))
    //    val result=rdd.map(Document.parse).map(doc=>{
    //      doc.put("a","b")
    //      doc
    //    })
    //    result.foreach(println)

    //    var resultStr=""
    //    val rdd=sc.parallelize(Array("hello"))
    //    val result=rdd.map(line=>{
    //      println("mark")
    //      resultStr+=line+"ddddd"
    //    })
    //    println(result.count())
    //    println(result.count())
    //    println(result.count())
    //    println(result.count())
    //    println(resultStr)

    //    rdd.count()
    //    System.setProperty("hadoop.home.dir", "E:\\64bit_software\\64bit_software\\hadoop-2.6.0-x64\\hadoop-2.6.0")
    //    val sparkConf=new SparkConf().setMaster("local").setAppName("hello_spark")
    //    val optionParam= mutable.HashMap[String,String]()
    //    optionParam.put(RocketMQConfig.NAME_SERVER_ADDR,"192.168.137.102:9876")
    //    val ssc=new JavaStreamingContext(sparkConf,Seconds(1))
    //    val topic=List("hello")
    ////    val data=ssc.socketTextStream("127.0.0.1",9527)
    ////    data.foreachRDD(line=>{
    ////      line.collect().foreach(println)
    ////    })
    //    val stream=RocketMqUtils.createJavaMQPullStream(ssc,"TagA",JavaConverters.seqAsJavaListConverter(topic).asJava,ConsumerStrategy.earliest,
    //      false,false,false,LocationStrategy.PreferConsistent,JavaConverters.mapAsJavaMapConverter(optionParam).asJava)
    ////    val dStream = RocketMqUtils.createMQPullStream(ssc, "TagA", "hello", ConsumerStrategy.earliest, true, false, false,JavaConverters.mapAsJavaMapConverter(optionParam).asJava)
    ////    dStream.map(message => message.getBody).print()
    //    stream.print()
    //    println("hello")
    //    ssc.start()
    //    ssc.awaitTermination()
  }

  def addColumn(document: Document): Document = {
    document.append("add.abb", 1)
  }

  def getIPAddress: IndexedSeq[Array[String]] = {
    val ipClass = new IP()
    ipClass.load("E:\\64bit_software\\64bit_software\\17monipdb\\17monipdb.dat")
    val st: Long = System.nanoTime
    var i: Int = 0
    val result = for (i <- 0 until 1000000) yield {
      ipClass.find(randomIp)
    }
    val et: Long = System.nanoTime
    System.out.println((et - st) / 1000 / 1000)
    result
  }
  def getCounts(msg:String): Int ={
    val doc=Document.parse(msg)
    doc.get("rows").asInstanceOf[java.util.ArrayList[Document]].count(line=>line.getString("table") == "order" || line.getString("table" ) == "user")
  }
}
