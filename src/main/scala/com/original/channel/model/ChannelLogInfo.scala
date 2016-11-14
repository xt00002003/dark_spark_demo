package com.original.channel.model

/**
  * sdk log info
  */
class ChannelLogInfo(
  // Mandatory
  val timestamp: Long, // epoch ms
  val ip: String,
  var device_id: String = null,
  var project_id: String = null,      //map to "appkey"
  var channel_id:String =null,
  var model:String=null,
  var language:String=null,
  var country:String=null,
  var pkg_name:String=null,
  var startup_list: Array[StartUpLogInfo] = null,
  var imei:String= null,
  var android_id: String = null,
  var mac : String = null,
  var uuid: String = null 
  
  // Optional
) extends Product with Serializable {

  // @formatter:on
  def this(timestamp: Long, ip: String) {
    this(timestamp, ip, null)
  }

  override def productArity: Int = 14

  override def canEqual(that: Any): Boolean = that.isInstanceOf[ChannelLogInfo]

  override def productElement(idx: Int): Any = idx match {
    case 0 => timestamp
    case 1 => ip
    case 2 => device_id
    case 3 => project_id
    case 4 => channel_id
    case 5 => model
    case 6 => language
    case 7 => country
    case 8 => pkg_name
    case 9 => startup_list
    case 10 => imei
    case 11 => android_id
    case 12 => mac
    case 13 => uuid
  }
}
