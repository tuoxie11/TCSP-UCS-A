package com.tuoxie.contrast

import com.alibaba.fastjson.JSONArray
import com.tuoxie.contrast.ExploreUtil._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, RangePartitioner, SparkConf}

import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

object DistributeUCS {

    final val INF: Double = 1000.0

    def main(args:Array[String]): Unit = {
        // 全局的SparkSession环境
//        val conf = new SparkConf().setMaster("local[*]").setAppName("UCS")
        val conf = new SparkConf().setAppName("UCS")
        val ss = SparkSession.builder.config(conf).getOrCreate()
        val sc = ss.sparkContext

        //广播变量
        val topMap1: Map[String, (Double, Double, List[String], Double, Double, List[String])] = getTopologyMap(sc)
        val topMap = sc.broadcast(topMap1)

        val roadLenTime1: Map[String, (Double, Double, String, String)] = readLenTime(sc)
        val roadLenTime = sc.broadcast(roadLenTime1)

        val pointTopologyMap1: Map[String, List[(String, String, Double)]] = getPointTopologyMap(sc)
        val pointTopologyMap = sc.broadcast(pointTopologyMap1)

        //定义一个初始值为0的累加器
        val my_acc = sc.longAccumulator("my_acc")
        val startTime = new Date().getTime

        var interval = 3
        var path:String = null
        if(args.length==2){
            interval = args(0).toInt
            path = args(1)
        }else{
            return
        }

        val trajectorys = sc.textFile("hdfs://namenode:8020"+path).coalesce(4)
//        val trajectorys = sc.textFile("file:///E:/电子地图/20160901/AT0474.txt")

        val value: RDD[List[String]] = trajectorys.mapPartitions(inter => {
            val iterator = inter.map(line => {
                //每一行处理数据
                val road = paresLine(line)
                if (road != "0") {
//                    val startTime1 = new Date().getTime
                    val res = executeAdjcentPointUCS(road, roadLenTime.value, pointTopologyMap.value, topMap.value, interval)
//                    val endTime1 = new Date().getTime
//                    val s1 = endTime1 - startTime1
//                    println(s"lll:\t$s1")

                    res.map(x=>{
                        val p = x._2.mkString(",")
                        s"${x._1}:$p"
                    })
                }else
                    List[String]()
            })
            iterator
        })

        value.foreachPartition(iter=>{
            iter.foreach(x=>{
                x.foreach(y=>{
//                    println(y)
                    my_acc.add(1)
                })
            })
        })

        val endTime = new Date().getTime
        val s = endTime - startTime
        println(s"lll:\t$s")
        println(my_acc.value)


    }

    /**
     * 搜索两个轨迹点的候选路段间的最短路径
     *
     * @param start            : 起始路段
     * @param geoMap           : geohash索引
     * @param globalVar        : geohash编码的全局变量
     * @param roadLenTime      : 路段id及其路段信息，长度，60km消耗时间，路段的第一个gps点个最后一个gps点
     * @param pointTopologyMap : 路网的点拓扑文件
     * @param topMap           : 路网的路段拓扑文件
     * @return 返回搜索路径的信息
     */
    def executeAdjcentPointUCS(start: String,
                               roadLenTime: Map[String, (Double, Double, String, String)],
                               pointTopologyMap: Map[String, List[(String, String, Double)]],
                               topMap: Map[String, (Double, Double, List[String], Double, Double, List[String])],
                               interval: Int): List[(Double, List[String])] = {
        //得到路段的两个端点
        val point1: String = roadLenTime(start)._3
        val point2: String = roadLenTime(start)._4

        //执行UCS算法
        val resMap: mutable.Map[String, (Double, List[String])] = UCS(point1, point2, interval, pointTopologyMap)
        resMap.values.toList
    }



    //得到当前没有固定的节点中，长度最短的那一个的下标

    /**
     * 根据路段ID，找到该路段在interval分钟时间间隔内所有可达的路段
     * roadLenTime: 全局变量，路段的代价信息  topMap:路段的拓扑信息
     *
     * @param solid  : 节点查看数组
     * @param length : 各节点代价情况
     * @return set: 找到的路段集合
     */
    def getMinIndex(solid: ListBuffer[Boolean], length: ListBuffer[Double]): Int = {
        var minIndex = -1
        var minValue = Double.MaxValue
        val indexs = solid.indices.filter(i => !solid(i))
        for (i <- indexs) {
            if (length(i) < minValue) {
                minValue = length(i)
                minIndex = i
            }
        }
        minIndex
    }

    /**
     * 根据路段ID，找到该路段在interval分钟时间间隔内所有可达的路段
     * roadLenTime: 全局变量，路段的代价信息  topMap:路段的拓扑信息
     *
     * @param v1     : 端点1
     * @param v2     : 端点2;
     * @param interval : 时间间隔，单位分钟
     * @param pointTopologyMap : 点拓扑文件
     * @return set: 找到的路段集合
     */
    def UCS(v1: String, v2: String, interval: Int, pointTopologyMap: Map[String, List[(String, String, Double)]]
           ): mutable.Map[String,(Double,List[String])] = {
        val limit = interval * 60.0 //这里是时间代价
        val nodes = ListBuffer[String]()
        val preNodes = ListBuffer[(String, String)]()
        val times = ListBuffer[Double]()
        val flags = ListBuffer[Boolean]()

        //将数据加到集合中，初始化集合
        // 加入两端节点
        nodes.append(v1)
        preNodes.append((v1, ""))
        times.append(0.0)
        flags.append(false)

        var minIndex: Int = getMinIndex(flags, times)
        while (minIndex != (-1)) {
            //遍历与节点相连的节点
            val ns: List[(String, String, Double)] = pointTopologyMap(nodes(minIndex))
            flags(minIndex) = true //固定当前节点
            for (n <- ns) {
                val index = nodes.indexOf(n._1)
                if (index != (-1)) {
                    //当前点存在集合中
                    val t = times(minIndex) + n._3 + 5.0
                    if (!flags(index) && t < times(index)) {
                        times(index) = t
                        preNodes(index) = (nodes(minIndex), n._2)
                    }
                } else {
                    //当前点不存在集合中
                    nodes.append(n._1)
                    preNodes.append((nodes(minIndex), n._2))
                    val t = times(minIndex) + n._3 + 5.0
                    times.append(t)
                    if (t > limit)
                        flags.append(true)
                    else
                        flags.append(false)
                }
            }
            minIndex = getMinIndex(flags, times)
        }

        //得到最短路径
        val res = mutable.Map[String,(Double,List[String])]()
        for(i <- 1 until nodes.length){
            var preIndex = i
            val path = ListBuffer[String]()
            breakable {
                while (preIndex != 0) {
                    if (!res.contains(nodes(preIndex))) {
                        path.insert(0, preNodes(preIndex)._2)
                        preIndex = nodes.indexOf(preNodes(preIndex)._1)
                    } else {
                        path.prependAll(res(nodes(preIndex))._2)
                        break()
                    }
                }
            }
            res.put(nodes(i),(times(i),path.toList))
        }
        res
    }


    /**
     * 根据两个路段的端点和GPS采样点 确定从哪个轨迹点开始搜索
     *
     * @param roadGPS1_1 : 路段的一个端点
     * @param roadGPS1_2 : 路段的另外一个端点
     * @param g1         : GSP采样点
     */
    def getPoint(roadGPS1_1: String, roadGPS1_2: String,g1: String): String = {

        val gps1_1: Array[Double] = roadGPS1_1.split(",").map(_.toDouble)
        val gps1_2: Array[Double] = roadGPS1_2.split(",").map(_.toDouble)
        val G1: Array[Double] = g1.split(",").map(_.toDouble)

        //两个路段上的投影点
        val touyinRoad1: (Double, Double) = touying(G1(0), G1(1), gps1_1(0), gps1_1(1), gps1_2(0), gps1_2(1))

        val d1 = getBigCirculDistance(gps1_1(0), gps1_1(1),touyinRoad1._1,touyinRoad1._2)
        val d2 = getBigCirculDistance( gps1_2(0), gps1_2(1),touyinRoad1._1,touyinRoad1._2)
        if(d1<=d2){
            roadGPS1_1
        }else{
            roadGPS1_2
        }
    }


    /**
     * 根据路段端点和GPS采样点计算投影点
     * x,y为
     *
     * @param x  : GSP采样点经度
     * @param y  : GSP采样点纬度
     * @param x1 : 路段的一个端点经度
     * @param y1 : 路段的一个端点纬度
     * @param x2 : 路段的另外一个端点经度
     * @param y2 : 路段的另外一个端点纬度
     * @return 投影点坐标
     */
    def touying(x: Double, y: Double, x1: Double, y1: Double, x2: Double, y2: Double): (Double, Double) = {
        val x0 = ((x2 - x1) * (x * (x2 - x1) + y * (y2 - y1)) + (y2 - y1) * (x1 * y2 - x2 * y1)) / ((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1))
        val y0 = ((y2 - y1) * (x * (x2 - x1) + y * (y2 - y1)) - (x2 - x1) * (x1 * y2 - x2 * y1)) / ((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1))
        //(x1,y1)与(x2,y2)之间的距离的平方
        val distance1 = (x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1)
        //(x1,y1)与(x0,y0)之间的距离的平方
        val distance2 = (x1 - x0) * (x1 - x0) + (y1 - y0) * (y1 - y0)
        //(x2,y2)与(x0,y0)之间的距离的平方
        val distance3 = (x2 - x0) * (x2 - x0) + (y2 - y0) * (y2 - y0)

        val result = new ArrayBuffer[Double]()
        if (Math.max(distance2, distance3) > distance1) {
            if (distance2 > distance3) {
                result += x2
                result += y2
            } else {
                result += x1
                result += y1
            }

        } else {
            result += x0
            result += y0
        }
        (result(0), result(1))
    }

    // 计算两点之间的距离

    /**
     * 根据两个经纬度点计算两点之间的地球大圆距离
     *
     * @param lon1 : GSP采样点经度
     * @param lat1 : GSP采样点纬度
     * @param lon2 : 路段的一个端点经度
     * @param lat2 : 路段的一个端点纬度
     * @return 经纬度坐标对应地球大圆距离
     */
    def getBigCirculDistance(lon1: Double, lat1: Double,
                             lon2: Double, lat2: Double): Double = {
        var ew1: Double = 1.0
        var ns1: Double = 1.0
        var ew2: Double = 1.0
        var ns2: Double = 1.0
        var distance: Double = 1.0
        val DEF_PI180: Double = 0.01745329252; // PI/180.0
        val DEF_R: Double = 6370693.5
        // 角度转换为弧度
        ew1 = lon1 * DEF_PI180
        ns1 = lat1 * DEF_PI180
        ew2 = lon2 * DEF_PI180
        ns2 = lat2 * DEF_PI180

        // 求大圆劣弧与球心所夹的角(弧度)
        distance = Math.sin(ns1) * Math.sin(ns2) + Math.cos(ns1) * Math.cos(ns2) * Math.cos(ew1 - ew2)
        // 调整到[-1..1]范围内，避免溢出
        if (distance > 1.0) {
            distance = 1.0
        } else {
            if (distance < -1.0) {
                distance = -1.0
            }
        }
        distance = DEF_R * Math.acos(distance)
        distance
    }


}
