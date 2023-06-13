package com.tuoxie.contrast

import com.alibaba.fastjson.JSONArray
import com.tuoxie.contrast.ExploreUtil._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkConf}

import java.io.FileWriter
import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

object DistributeDijstra {

    final val INF: Double = 1000.0
    final val errorFile = new FileWriter("./errorRoads.txt",false)
    final val findRoadTime = new FileWriter("./findRoadTime.txt",false)

    def main(args: Array[String]): Unit = {
        // 全局的SparkSession环境
        val conf = new SparkConf().setMaster("local[*]").setAppName("dijstra")
//        val conf = new SparkConf().setAppName("dijstra")
        val ss = SparkSession.builder.config(conf).getOrCreate()
        val sc = ss.sparkContext

        //广播变量
        val topMap1: Map[String, (Double, Double, List[String], Double, Double, List[String])] = getTopologyMap(sc)
        val topMap = sc.broadcast(topMap1).value

        val roadLenTime1: Map[String, (Double, Double, String, String)] = readLenTime(sc)
        val roadLenTime = sc.broadcast(roadLenTime1).value

        val pointTopologyMap1: Map[String, List[(String, String, Double)]] = getPointTopologyMap(sc)
        val pointTopologyMap = sc.broadcast(pointTopologyMap1).value

        //定义一个初始值为0的累加器
        val my_acc = sc.longAccumulator("my_acc")

        val startTime = new Date().getTime
        var interval = 30
        var path:String = null
//        if(args.length==2){
//            interval = args(0).toInt
//            path = args(1)
//        }else{
//            return
//        }

//        val trajectorys = sc.textFile("hdfs://namenode:8020"+path).coalesce(4)
        val trajectorys = sc.textFile("file:///E:/电子地图/20160901/AT0474.txt")
        val value: RDD[List[(String,String)]] = trajectorys.mapPartitions(inter => {
            val iterator = inter.map(line => {
                //每一行处理数据
                val road = paresLine(line)
                if (road != "0") {
                    println("\n")
//                    val startTime1 = new Date().getTime
                    val res = executeAdjcentPoint(road, roadLenTime, pointTopologyMap, topMap, interval)
//                    val endTime1 = new Date().getTime
//                    val s1 = endTime1 - startTime1
//                    println(s"lll:\t$s1")

                    res.map(x=>{
                        val p = x._2.mkString(",")
                        s"${x._1}:$p"
                        println(p)
                        (x._2.head,p)
                    })
                }else
                    List[(String,String)]()
            })
            iterator
        })


        value.foreachPartition(iter=>{
            iter.foreach(x=>{
                breakable{
                    x.foreach(y=>{
//                        println(y._2)
                        my_acc.add(1)
                    })
//                    println("\n")
                }
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
     * @param start               : 起始路段
     * @param roadLenTime      : 路段id及其路段信息，长度，60km消耗时间，路段的第一个gps点个最后一个gps点
     * @param pointTopologyMap : 路网的点拓扑文件
     * @param topMap           : 路网的路段拓扑文件
     * @return 返回搜索路径的信息
     */
    def executeAdjcentPoint(start: String,
                            roadLenTime: Map[String, (Double, Double, String, String)],
                            pointTopologyMap: Map[String, List[(String, String, Double)]],
                            topMap: Map[String, (Double, Double, List[String], Double, Double, List[String])],
                            interval:Int):  List[(Double, List[String])] = {


        //从数据库获取起始路段周围的路段
//        val roads = selectByRoadInterval(roads1(i), 2)
        //采用深度优先遍历
//        val roads:Array[String] = findRoads(start, interval,roadLenTime,topMap).toArray
        //采用广度优先遍历
        val roads:Array[String] = findRoadsByBFS(start, interval,roadLenTime,topMap).toArray
        //调用getMatrix方法初始化邻接矩阵
        val points = getPoints(roads, roadLenTime)
        val matrix = getMatrix(points, pointTopologyMap)
        //执行dijstra算法
        //起始点
        val point1: String = roadLenTime(start)._3
        val timePathList = dijstra(point1, points, matrix)

        val tuples: List[(Double, List[String])] = timePathList.map(t => {
            val res: ListBuffer[String] = new ListBuffer[String]()
            for (i <- 0 until t._2.length - 1) {
                val roads = pointTopologyMap(t._2(i))
                for (road <- roads) {
                    if (road._1 == t._2(i + 1))
                        res.append(road._2)
                }
            }
            if(!start.equals(res.head))
                res.insert(0,start)

            (t._1, res.toList)
        })
        tuples
    }

//    def printPath(points: List[String], pointTopologyMap: Map[String, List[(String, String, Double)]]): List[String] = {
//        val res: ListBuffer[String] = new ListBuffer[String]()
//        for (i <- 0 until points.length - 1) {
//            val roads = pointTopologyMap(points(i))
//            for (road <- roads) {
//                if (road._1 == points(i + 1))
//                    res.append(road._2)
//            }
//        }
//        res.toList
//    }


    /**
     * 根据两个路段的端点和GPS采样的两个点 确定 哪两个轨迹点之间的最短距离
     *
     * @param roadGPS1_1 : 路段1的一个端点
     * @param roadGPS1_2 : 路段1的另外一个端点
     * @param roadGPS2_1 : 路段2的一个端点
     * @param roadGPS2_2 : 路段2的另外一个端点
     * @param g1         : GSP采样点
     * @param g2         : GSP另外一个采样点
     */
    def getPoint(roadGPS1_1: String, roadGPS1_2: String, roadGPS2_1: String, roadGPS2_2: String,
                 g1: (Double, Double), g2: (Double, Double)): (String, String) = {

        val gps1_1: Array[Double] = roadGPS1_1.split(",").map(_.toDouble)
        val gps1_2: Array[Double] = roadGPS1_2.split(",").map(_.toDouble)
        val gps2_1: Array[Double] = roadGPS2_1.split(",").map(_.toDouble)
        val gps2_2: Array[Double] = roadGPS2_2.split(",").map(_.toDouble)

        //两个路段上的投影点
        val touyinRoad1 = touying(g1._1, g1._2, gps1_1(0), gps1_1(1), gps1_2(0), gps1_2(1))
        val touyinRoad2 = touying(g2._1, g2._2, gps2_1(0), gps2_1(1), gps2_2(0), gps2_2(1))

        //前一个路段投影点到后一个GPS点的向量及其模
        val touyinToCurGPSVector = (g2._1 - touyinRoad1._1, g2._2 - touyinRoad1._2)
        val model1: Double = math.pow(math.pow(touyinToCurGPSVector._1, 2) + math.pow(touyinToCurGPSVector._2, 2), 0.5)
        val touyinToRoad1_1Vector = (gps1_1(0) - touyinRoad1._1, gps1_1(1) - touyinRoad1._2)
        val model2: Double = math.pow(math.pow(touyinToRoad1_1Vector._1, 2.0) + math.pow(touyinToRoad1_1Vector._2, 2.0), 0.5)
        val touyinToRoad1_2Vector = (gps1_2(0) - touyinRoad1._1, gps1_2(1) - touyinRoad1._2)
        val model3: Double = math.pow(math.pow(touyinToRoad1_2Vector._1, 2.0) + math.pow(touyinToRoad1_2Vector._2, 2.0), 0.5)

        val angle1 = math.acos((touyinToCurGPSVector._1 * touyinToRoad1_1Vector._1 + touyinToCurGPSVector._2
            * touyinToRoad1_1Vector._2) / (model1 * model2))
        val angle2 = math.acos((touyinToCurGPSVector._1 * touyinToRoad1_2Vector._1 + touyinToCurGPSVector._2
            * touyinToRoad1_2Vector._2) / (model1 * model3))
        //根据角度比较得到前一个路段最近的点
        var p1: String = null
        if (angle1 < angle2) {
            p1 = roadGPS1_1
        } else {
            p1 = roadGPS1_2
        }

        //后一个路段投影点到前一个GPS点的向量及其模
        val touyinToPreGPSVector = (g1._1 - touyinRoad2._1, g1._2 - touyinRoad2._2)
        val model4: Double = math.pow(math.pow(touyinToPreGPSVector._1, 2) + math.pow(touyinToPreGPSVector._2, 2), 0.5)
        val touyinToRoad2_1Vector = (gps2_1(0) - touyinRoad2._1, gps2_1(1) - touyinRoad2._2)
        val model5: Double = math.pow(math.pow(touyinToRoad2_1Vector._1, 2.0) + math.pow(touyinToRoad2_1Vector._2, 2.0), 0.5)
        val touyinToRoad2_2Vector = (gps2_2(0) - touyinRoad2._1, gps2_2(1) - touyinRoad2._2)
        val model6: Double = math.pow(math.pow(touyinToRoad2_2Vector._1, 2.0) + math.pow(touyinToRoad2_2Vector._2, 2.0), 0.5)

        val angle3 = math.acos((touyinToPreGPSVector._1 * touyinToRoad2_1Vector._1 + touyinToPreGPSVector._2
            * touyinToRoad2_1Vector._2) / (model4 * model5))
        val angle4 = math.acos((touyinToPreGPSVector._1 * touyinToRoad2_2Vector._1 + touyinToPreGPSVector._2
            * touyinToRoad2_2Vector._2) / (model4 * model6))
        //根据角度比较得到前一个路段最近的点
        var p2: String = null
        if (angle3 < angle4) {
            p2 = roadGPS2_1
        } else {
            p2 = roadGPS2_2
        }
        (p1, p2)
    }

    /**
     * 根据路段集合和路段信息map，得到路段集合中所有的端点
     *
     * @param roads       : 路段集合
     * @param roadLenTime : 全局路段信息
     */
    def getPoints(roads: Array[String], roadLenTime: Map[String, (Double, Double, String, String)]): Array[String] = {
        val ps = mutable.Set[String]()
        for (road <- roads) {
            ps.add(roadLenTime(road)._3)
            ps.add(roadLenTime(road)._4)
        }
        ps.toArray

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

    /**
     * 根据路段数组和Topology文件生成的map来生成矩阵
     *
     * @param points           : 点数组
     * @param pointTopologyMap : PointTopology文件生成的点Map结构
     */
    def getMatrix(points: Array[String], pointTopologyMap: Map[String, List[(String, String, Double)]]): Array[Array[Double]] = {
        //定义及初始化矩阵
        val matrix = new Array[Array[Double]](points.length)
        for (i <- points.indices) {
            matrix(i) = new Array[Double](points.length)
            for (j <- points.indices) {
                if (i == j)
                    matrix(i)(j) = 0.0
                else
                    matrix(i)(j) = INF
            }
        }

        //根据数据文件对矩阵赋值
        for (i <- points.indices) {
            val tmp = pointTopologyMap.getOrElse(points(i), List[(String, String, Double)]())
            for (road <- tmp) {
                if (points.contains(road._1)) { //如果不在points的点将不考虑
                    val index = points.indexOf(road._1)
                    matrix(i)(index) = road._3
                }
            }
        }
        matrix
    }

    /**
     * 根据起始路段、终止路段、路段的代价信息、邻接矩阵生成起始路段到终止路段的最短距离和路径
     *
     * @param startPoint : 起始点
     * @param points     : 点数组
     * @param matrix     : 邻接矩阵
     */
    def dijstra(startPoint: String, points: Array[String], matrix: Array[Array[Double]]): List[(Double,List[String])] = {
        val startIndex = points.indexOf(startPoint)

        var min: Double = 0.0
        var minIndex: Int = -1
        var j: Int = 0

        //初始化标记数组和距离数组
        val path = new Array[Int](points.length).map(_ => -1)
        val flag = new Array[Boolean](points.length).map(_ => false)
        val dis = new Array[Double](points.length).map(_ => INF)
        for (i <- matrix(startIndex).indices) {
            dis(i) = matrix(startIndex)(i)
            if (matrix(startIndex)(i) != INF && matrix(startIndex)(i) != 0.0)
                path(i) = startIndex
        }
        //将起点置为已查看
        flag(startIndex) = true

        //遍历其它n-1个没有被确定的点
        for (_ <- 1 until flag.length) {
            //找到距离起点最近的点，且没被查看
            min = INF
            j = 0
            while (j < points.length) {
                if ((!flag(j)) && dis(j) < min) {
                    min = dis(j)
                    minIndex = j
                }
                j += 1
            }
            //置为true，表示已查看，并用其距离去更新其它距离
            flag(minIndex) = true
            for (m <- points.indices) {
                if (!flag(m) && dis(m) > min + matrix(minIndex)(m)) {
                    path(m) = minIndex
                    dis(m) = min + matrix(minIndex)(m)
                }
            }
        }

        val res = ListBuffer[(Double,List[String])]()
        for(i <- points.indices if i!= startIndex){
            j = i
            val p = ListBuffer[String]()
            while (path(j) != (-1)) {
                p.insert(0,points(j))
                j = path(j)
            }
            p.insert(0,points(startIndex))
            res.append((dis(i), p.toList))
        }
        res.toList
    }


    case class MyRoad(id:String,time: Double,pre:String)

    /**
     * 根据路段ID，找到该路段在interval分钟时间间隔内所有可达的路段
     * roadLenTime: 全局变量，路段的代价信息  topMap:路段的拓扑信息
     * @param road: 路段ID
     * @param interval: 时间间隔，单位分钟
     * @return set: 找到的路段集合
     */
    def findRoads(road: String, interval: Int=30,roadLenTime: Map[String, (Double, Double, String, String)],topMap: Map[String, (Double, Double, List[String], Double, Double, List[String])]): Set[String] = {
        val res = mutable.Set[String]()
        val tmp = mutable.Map[String,Double]()
            res.add(road)
        val stack = mutable.Stack[MyRoad]()
        stack.push(MyRoad(road,0.0,""))

        while (stack.nonEmpty) {
            breakable {
                val tuple = stack.pop()
                tmp.put(tuple.id,tuple.time)
                try {
                    //路段两端相邻的路段
                    val roads: ListBuffer[String] = ListBuffer[String]()
                    if (!topMap(tuple.id)._3.contains(tuple.pre))
                        roads ++= topMap(tuple.id)._3
                    if (!topMap(tuple.id)._6.contains(tuple.pre))
                        roads ++= topMap(tuple.id)._6

                    for (r <- roads) {
                        //路口时间为5
                        val time = tuple.time + 5 + roadLenTime(r)._2
                        if(tmp.getOrElse(r,INF)>time){
                            tmp(r)=time
                            if (time <= interval.toDouble) {
                                res.add(r)
                                stack.push(MyRoad(r, time, tuple.id))
                            }else if(time > interval.toDouble){
                                res.add(r)
                            }
                        }
                    }
                } catch {
                    case e: Exception =>
                        println(tuple)
                        e.printStackTrace()
                        break
                }
            }
        }
        res.toSet
    }


    /**
     * 根据路段ID，找到该路段在interval分钟时间间隔内所有可达的路段
     * roadLenTime: 全局变量，路段的代价信息  topMap:路段的拓扑信息
     * @param road: 路段ID
     * @param interval: 时间间隔，单位分钟
     * @return set: 找到的路段集合
     */
    def findRoadsByBFS(road: String, interval: Int=30,roadLenTime: Map[String, (Double, Double, String, String)],
                         topMap: Map[String, (Double, Double, List[String], Double, Double, List[String])]): Set[String] = {
        val res = mutable.Set[String]()
        val tmp = mutable.Map[String,Double]()
        res.add(road)
        val queue = mutable.Queue[MyRoad]()
        queue.enqueue(MyRoad(road,0.0,""))
        while (queue.nonEmpty) {
            breakable {
                val myRoad = queue.dequeue()
                tmp.put(myRoad.id,myRoad.time)
                try {
                    //路段两端相邻的路段
                    val roads: ListBuffer[String] = ListBuffer[String]()
                    if (!topMap(myRoad.id)._3.contains(myRoad.pre))
                        roads ++= topMap(myRoad.id)._3
                    if (!topMap(myRoad.id)._6.contains(myRoad.pre))
                        roads ++= topMap(myRoad.id)._6
                    for (r <- roads) {
                        //路口时间为5
                        val time = myRoad.time + 5 + roadLenTime(r)._2
                        if(tmp.getOrElse(r,INF)>time){
                            tmp(r)=time
                            if (time <= interval.toDouble) {
                                res.add(r)
                                queue.enqueue(MyRoad(r, time, myRoad.id))
                            }else if(time > interval.toDouble){
                                res.add(r)
                            }
                        }
                    }
                } catch {
                    case e: Exception =>
                        e.printStackTrace()
                        break
                }
            }
        }
        res.toSet
    }

}
