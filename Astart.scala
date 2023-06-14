package tt

import java.io.{File, FileWriter}
import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source.fromFile

object Astart {

    /**
     * 解析轨迹行数据，返回轨迹点经纬度二元组
     *
     * @param line : String 轨迹行数据
     * @return (Double,Double): 轨迹点经纬度坐标
     */
    def paresLine(line: String): String = {
        val words = line.split(",")
        words(8)
    }

//    val connection = getConnection()
    def getConnection():Connection={
        val url = "jdbc:postgresql://localhost:5432/route_q"
        val username = "postgres"
        val password = "root14"

        // 建立连接
        val connection: Connection = DriverManager.getConnection(url, username, password)
        connection
    }

    /**
     * 使用A*算法对单个路段一定距离范围内所有路段的最短路径集合
     *
     * @param roadId     路段id
     * @param distance 距离范围，单位米
     * @return  返回为最短路径组成的list
     */
//    def astartFun(roadId: String, distance: Double): List[String] = {
//        val statement = connection.createStatement()
//        val resultSet = statement.executeQuery("SELECT * FROM your_table")
//
//        // 处理查询结果
//        while (resultSet.next()) {
//            // 从结果集中获取数据
//            val columnValue = resultSet.getString("column_name")
//            // 进行其他操作
//            println(columnValue)
//        }

//        // 关闭连接
//        resultSet.close()
//        statement.close()
//        connection.close()
//
//        List[String](roadId)
//    }
def astartFun(roadId: String, distance: Double,connection:Connection): List[String] = {
//    val connection = DriverManager.getConnection(conn_str, "postgres", "root14")
    val sql1 = s"SELECT roadid FROM edge_table_copy1 WHERE st_dwithin(the_geog, (SELECT the_geog FROM edge_table WHERE roadid = '$roadId'), $distance, true) and roadid !='$roadId';"
    //执行SQL语句并获取结果集 范围查询
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql1)
    val resultList = new ListBuffer[Tuple2[String,String]]()

    while (resultSet.next()) {
        val a =resultSet.getString("roadid")
        val t=(roadId,a)
        resultList.append(t)    // 两两一组
    }
    //    println(resultList)
    // 将查询路段转为端点
    var p_list = ListBuffer[List[Any]]()  // 创建可变 List，存储端点
    for (x <- resultList) {
        val sql2 = s"select target from edge_table_copy1 where roadid = '${x._1}';"
        val sql3 = s"select source from edge_table_copy1 where roadid = '${x._2}';"
        var p_two = ListBuffer[Any]()  // 存储一对路段的端点
        val res1 = statement.executeQuery(sql2)
        while (res1.next()) {
            p_two.append(res1.getInt("target")) // 开始路段的终点端点
        }
        val res2 = statement.executeQuery(sql3)
        while (res2.next()) {
            p_two.append(res2.getInt("source")) // 结束路段的起始端点
        }
        p_list += p_two.toList  // 将可变 List 转换为不可变 List，并添加到 p_list 中
    }
    //    println(p_list)
    // 使用A*算法进行路径查询
    val routeList = new ListBuffer[String]()
    val timeList = new ListBuffer[Double]()
    for (p <- p_list){
        val startTime1 = System.currentTimeMillis()
//        val sql4 = s"SELECT array_agg(b.roadid) FROM pgr_astar( 'SELECT id, source, target, cost, reverse_cost,x1,y1,x2,y2 FROM edge_table',${p(0)} ,${p(1)}) aa join  edge_table b on aa.edge=b.id;"
        val sql4 = s"SELECT array_agg(edge) FROM pgr_astar( 'SELECT id, source, target, cost, reverse_cost,x1,y1,x2,y2 FROM edge_table_copy1', ${p(0)} ,${p(1)});"
        val res4 = statement.executeQuery(sql4)
        while (res4.next()) {
            val temp = res4.getArray("array_agg")
            if(temp != null){
                val array = temp.toString.replaceAll("[{}]", "")
                 //获取路径，转为字符串
                routeList += array
            }

        }
        val endTime1 = System.currentTimeMillis()
        val s1 = endTime1 - startTime1
        timeList.append(s1.toDouble/1000)
//        println(s"${s1.toDouble/1000}")

    }
    fw.write(s"${timeList.sum/timeList.size}\n")



    fw.flush()
    resultSet.close()
    statement.close()

    //    List[String](roadId)
    routeList.toList
}

/*
2000	500.0:	1450.498
4000	500.0:	2856.078
*/
val fw = new FileWriter("output.txt", false)
    def main(args: Array[String]): Unit = {
        val fileNums = Array(2000,4000,6000,8000,10000,12000)
        val time = Array(3)
        val distances = time.map(minute=>{300.0})

        val path = new File(s"D:\\mysoftware\\Tencent\\QQ\\mess_records\\1593938883\\FileRecv\\3min")

        val conf = new SparkConf().setMaster("local[*]").setAppName("path_search")
        val context = new SparkContext(conf)
//        val connectionB = context.broadcast(getConnection())

        //遍历时间限制和数据量
        for(fileNum <- fileNums){
            val source = fromFile(s"D:\\mysoftware\\Tencent\\QQ\\mess_records\\1593938883\\FileRecv\\tradata\\${fileNum}.txt")
            val lines = source.getLines().toList
            val linesRDD = context.makeRDD(lines)
//            val startTime1 = System.currentTimeMillis()

            val res: RDD[List[String]] = linesRDD.map(line => {

                //每一行处理数据
                val road = paresLine(line)
                println(road)
                if (road != "0") {
                    val connection = getConnection()
                    //这个方法是对每个路段进行搜索
                    val pathes: List[String] = astartFun(road, 1000.0,connection)
                    connection.close()
                    pathes
                } else {
                    List[String]()
                }
            })

            //打印每个最短路径
            res.flatMap(x=>x).foreach(path => {
                println(path)
            })

//            val endTime1 = System.currentTimeMillis()
//            val s1 = endTime1 - startTime1
//            println(s"$fileNum\t500.0:\t${s1.toDouble/1000}")

//            fw.write(s"$fileNum\t500.0:\t${s1.toDouble/1000}\n")
//            fw.flush()
            source.close()
        }
        fw.close()
//        connection.close()
    }
//    2000	500.0:	1450.498
//    4000	500.0:	2856.078
//    2000	500.0:	753.709
//    4000	500.0:	1480.732
}
