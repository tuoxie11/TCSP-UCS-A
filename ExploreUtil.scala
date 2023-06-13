package com.tuoxie.contrast

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object ExploreUtil {
    private final val path:String = "file:///D:/2021-2022/数据备份/地图数据/"
//    private final val path:String = "hdfs://namenode:8020/"
    /**
     * 从hdfs中geoHash.json文件数据,获得Geohash文件数据
     *
     * @param sc SparkContext
     * @return geoMap 网格到路段的数据
     */
    def readGeohash(sc: SparkContext): Map[String, JSONArray] = {
        val geoHashRDD = sc.textFile(path+"geoHash.json").collect()
        //        val geoHashRDD = sc.textFile("hdfs://namenode:8020/tuoxie/input/geoHash.json").collect()
        val geoMap = mutable.Map[String, JSONArray]()
        geoHashRDD.foreach(x => {
            val jsonObj = JSON.parseObject(x)
            geoMap.put(jsonObj.getString("GeoHash"), jsonObj.getJSONArray("nodes"))
        })
        println("geohash读取完毕！")
        geoMap.toMap
    }

    /**
     * 构成Geohash编码的全局变量
     *
     * @return tuple 网格到路段的数据
     */
    def globalVariable(): (String, Double, Double, Double, Double) = {
        val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
        //最大经纬度，其中经度18位，维度17位
        val MAXLONGITUDE = 180.0
        val MAXLATITUDE = 90.0
        val MINLONGITUDE: Double = -180.0
        val MINLATITUDE: Double = -90.0
        (base32, MAXLONGITUDE, MAXLATITUDE, MINLONGITUDE, MINLATITUDE)
    }

    /**
     * 返回路段长度和时间损耗数据
     *
     * @return Map[String,(Double,Double,String,String)] 路段相关信息(roadID，(长度,60km/h时间代价,一端经纬度点，另一端经纬度点))
     */
    def readLenTime(sc: SparkContext): Map[String, (Double, Double,String,String)] = {
        val res = mutable.Map[String, (Double, Double,String,String)]()
        val roadLenTimeRDD:RDD[String] = sc.textFile(path+"city202202.txt")
        val list = roadLenTimeRDD.collect().toList
        list.foreach(l => {
            val words = l.trim.split(";")
            val time = (words.last.toDouble * 3600 / 60).formatted("%.3f").toDouble
            val gpss =words(2).split("、")
            res(words(0)) = (words.last.toDouble, time,gpss(0),gpss.last)
        })
        res.toMap
    }

    // 取得候选路段
    /**
     * 根据经纬度点获取候选路段集合
     *
     * @param gps: (Double, Double) 经纬度坐标点
     * @param geoMap: 网格到路段集合的映射
     * @param globalVar: geohash编码所需参数
     * @return List: 网格到路段的数据为 List(roadID,List(LonLat))
     */
    def getCondidates(gps: (Double, Double), geoMap: Map[String, JSONArray],
                      globalVar: (String, Double, Double, Double, Double)): List[(String, List[String])] = {
        val trajeolon1 = gps._1 - 0.00068
        val trajeolon2 = gps._1 + 0.00068
        val trajeolat1 = gps._2 - 0.00068
        val trajeolat2 = gps._2 + 0.00068

        //下面添加候选路段的geohash网格
        //现在计算四个geohash网格
        val GeoHashId1 = GeoHashEnCode((trajeolon1, trajeolat2), globalVar)
        val GeoHashId2 = GeoHashEnCode((trajeolon2, trajeolat2), globalVar)
        val GeoHashId3 = GeoHashEnCode((trajeolon1, trajeolat1), globalVar)
        val GeoHashId4 = GeoHashEnCode((trajeolon2, trajeolat1), globalVar)
        val geos: ListBuffer[String] = ListBuffer()
        geos.append(GeoHashId1)
        geos.append(GeoHashId2)
        geos.append(GeoHashId3)
        geos.append(GeoHashId4)

        val map = mutable.Map[String, List[String]]()
        for (geo <- geos) {
            val jsonArr: JSONArray = geoMap.getOrElse(geo, null)
            if (jsonArr != null) {
                jsonArr.toArray().foreach(x => {
                    val obj = x.asInstanceOf[JSONObject]
                    val l = ListBuffer[String]()
                    obj.getJSONArray("LonLats").toArray().foreach(t => {
                        l.append(t.asInstanceOf[String])
                    })
                    map.put(obj.getString("RoadId"), l.toList)
                })
            }
        }
        map.map(x => {
            (x._1, x._2)
        }).toList
    }

    // 获取7个字符换的geohash编码 lon是经度， lat是纬度
    /**
     * 根据GPS轨迹点获得对应的GeoHash编码字符串
     * @param GPS: 经纬度坐标二元组
     * @return string: 7字符geohash编码字符串
     */
    def GeoHashEnCode(GPS: (Double, Double), globalVar: (String, Double, Double, Double, Double)): String = {
        /*
        * 将经纬度点转换为二进制表示
        * 输入：经纬度点
        * 输出：二进制表示
        * */
        var maxLong = globalVar._2
        var maxLat = globalVar._3
        var minLong = globalVar._4
        var minLat = globalVar._5
        val long: Double = GPS._1.formatted("%.7f").toDouble
        val lat: Double = GPS._2.formatted("%.7f").toDouble
        var s = ""
        var midLong: Double = 0.0
        var midLat: Double = 0.0
        for (i <- 1 to 18) {
            midLong = ((maxLong + minLong) / 2).formatted("%.7f").toDouble
            midLat = ((maxLat + minLat) / 2).formatted("%.7f").toDouble
            if (long >= midLong) {
                s += "1"
                minLong = midLong
            } else {
                s += "0"
                maxLong = midLong
            }
            if (i < 18) {
                if (lat >= midLat) {
                    s += "1"
                    minLat = midLat
                } else {
                    s += "0"
                    maxLat = midLat
                }
            }
        }
        byToString(s, globalVar._1)
    }
    def byToString(bys: String, base32: String): String = {
        /*
        * 将二进制字符串转换为编码字符
        * 输入：二进制字符串
        * 输出：GeoHash网格字符编码
        * */
        var res = ""
        val index = Range(0, 36, 5)
        for (i <- 0 to 6) {
            val subS = bys.substring(index(i), index(i + 1))
            res += base32(Integer.parseInt(subS, 2))
        }
        res
    }

    /**
     * 解析轨迹行数据，返回轨迹点经纬度二元组
     *
     * @param line  : String 轨迹行数据
     * @return (Double,Double): 轨迹点经纬度坐标
     */
    def paresLine(line: String): String = {
        val words = line.split(",")
        words(8)
    }

    /**
     * 解析点拓扑文件返回 点的拓扑map
     */
    def getPointTopologyMap(sc: SparkContext):Map[String,List[(String,String,Double)]]={
        val pointTopoRDD:RDD[String] = sc.textFile(path+"pointTopology2023.txt")
        val lines = pointTopoRDD.collect().toList
        lines.map(line=>{
            val words = line.trim.split("\\|")
            val list = words(1).split(";").map(t => {
                val ss = t.split(",")
                //将长度换算成时间 60km/h
                (ss(0) + "," + ss(1), ss(2), ss(3).toDouble*60)
            }).toList
            (words(0),list)
        }).toMap
    }

    /**
     * 对西安市路段拓扑文件构建映射Map
     *
     * @return Map: 网格到路段的数据
     */
    def getTopologyMap(sc: SparkContext): Map[String, (Double, Double, List[String], Double, Double, List[String])] = {
        val roadTopoRDD:RDD[String] = sc.textFile(path+"Topology202202.txt")
        val lines = roadTopoRDD.collect().toList
        val topologyMap: mutable.Map[String, (Double, Double, List[String], Double, Double, List[String])]
        = mutable.Map[String, (Double, Double, List[String], Double, Double, List[String])]()
        for (line <- lines) {
            //51493004764,109.117634,34.298574,109.117638,34.297455
            val array = line.trim.split('|')
            val words = array(0).split(",")

            //109.117634,34.298574:51493004765,51493004766
            val StartArray = new ListBuffer[String]()
            val StartArrayStr = array(1).split(":")
            if (StartArrayStr.length != 1) {
                val TempArray = StartArrayStr(1).split(",")
                for (e <- TempArray) {
                    StartArray += e
                }
            }
            val EndArray = new ListBuffer[String]()
            val EndArrayStr = array(2).split(":")
            if (EndArrayStr.length != 1) {
                val TempArray = EndArrayStr(1).split(",")
                for (e <- TempArray) {
                    EndArray += e
                }
            }
            topologyMap += (words(0) -> (words(1).toDouble, words(2).toDouble, StartArray.toList,
                words(3).toDouble, words(4).toDouble, EndArray.toList))
        }
        println("topology读取完毕！")
        topologyMap.toMap
    }


}
