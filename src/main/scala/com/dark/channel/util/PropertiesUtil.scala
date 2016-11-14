package com.dark.channel.util
import java.util.Properties
import java.io.FileInputStream
/**
  * Created by darkxue on 14/11/16.
  */
object PropertiesUtil {
   def getValue(key:String):String={
     //文件要放到resource文件夹下
     val properties = new Properties()
     val path = Thread.currentThread().getContextClassLoader.getResource("serverConfig.properties").getPath
     properties.load(new FileInputStream(path))
     properties.getProperty(key)
   }
}
