package com.ijunhai.common.monitor


  object MonitorType extends Enumeration {
    type MonitorType = Value //声明枚举对外暴露的变量类型
    val OTHER = Value(0,"1")
    val FILTER = Value("2")
    val DEST = Value("3")
    val COPY = Value("4")
    val TEST = Value("5")


    def checkExists(day: String) = this.values.exists(_.toString == day) //检测是否存在此枚举值
    def isWorkingDay(day: MonitorType) = !(day == OTHER || day == FILTER) //判断是否是工作日
    def showAll = this.values.foreach(println) // 打印所有的枚举值
  }


