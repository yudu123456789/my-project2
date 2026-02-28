import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object HealthCodeSimulator {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: HealthCodeSimulator <cdinfo_path> <infected_path> <output_path>")
      sys.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Health Code Red Simulation")
      .getOrCreate()

    import spark.implicits._

    // --- 第一步：读入文件 ---
    // cdinfo.txt: 基站编号, 时间, 状态(1进2出), 手机号
    val cdinfo = spark.read.option("header", "false")
      .csv(args(0))
      .toDF("base_id", "time", "status", "phone")

    // infected.txt: 被感染人员手机号
    val infectedList = spark.read.option("header", "false")
      .csv(args(1))
      .toDF("infected_phone")

    // --- 第二步 & 第三步：构建所有人的进出站轨迹区间 ---
    // 为了处理PDF中提到的“同一人多次进出”问题，使用窗口函数进行匹配
    val windowSpec = Window.partitionBy("phone", "base_id").orderBy("time")

    // 为每条记录生成序号，方便进出站配对 (1对应进，2对应出)
    val sessionDf = cdinfo.withColumn("rank", row_number().over(windowSpec))
    
    // 将进站(1)和出站(2)转为同一行记录：[手机号, 基站, 进站时间, 出站时间]
    val entries = sessionDf.filter($"status" === "1").select($"phone", $"base_id", $"time".as("start_time"), $"rank")
    val exits = sessionDf.filter($"status" === "2").select($"phone".as("p2"), $"base_id".as("b2"), $"time".as("end_time"), $"rank")

    // 通过手机号、基站和序号连接，确保时间段一一对应
    val allTrajectories = entries.join(exits, 
        entries("phone") === exits("p2") && 
        entries("base_id") === exits("b2") && 
        entries("rank") === exits("rank")
      )
      .select(entries("phone"), entries("base_id"), $"start_time", $"end_time")

    // --- 第四步：筛选感染者轨迹并进行空间/时间碰撞 ---
    
    // 1. 提取感染者的轨迹
    val infectedTrajectories = allTrajectories.join(infectedList, 
        allTrajectories("phone") === infectedList("infected_phone"))
      .select($"base_id", $"start_time".as("inf_start"), $"end_time".as("inf_end"))

    // 2. 核心算法：连接全量数据与感染者轨迹
    // 条件：基站相同 AND (潜在感染者进站 <= 感染者离站 AND 潜在感染者离站 >= 感染者进站)
    val redCodeList = allTrajectories.join(infectedTrajectories, "base_id")
      .filter(
        $"start_time" <= $"inf_end" && $"end_time" >= $"inf_start"
      )
      // 排除感染者本人
      .join(infectedList, allTrajectories("phone") === infectedList("infected_phone"), "left_anti")
      .select($"phone").distinct()

    // --- 结果导出 ---
    redCodeList.write.mode("overwrite").text(args(2))

    spark.stop()
  }
}
