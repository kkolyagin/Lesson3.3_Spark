import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.functions.{col, desc, from_unixtime, lit}

//import scala.reflect.internal.util.TriState.False

object Main extends App{

    var spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark ex3.2")
      .getOrCreate()
    //import spark.implicits._

    val schema = new StructType()
      .add("id", IntegerType)
      .add("timestamp", IntegerType)
      .add("type", StringType)
      .add("page_id", IntegerType)
      .add("tag", StringType)
      .add("sign", BooleanType)

    val schema2 = new StructType()
      .add("id", IntegerType)
      .add("user_id", IntegerType)
      .add("fio", StringType)
      .add("dateofcreate", IntegerType)


    val data = Seq(Row(12345, 1667627426, "click", 101, "Sport", true),
        Row(12345, 1438732800, "scroll", 101, "sport", true),
        Row(12345, 1380412800, "move", 102, "medicine", true),
        Row(12348, 1211760000, "visit", 103, "hitech", true),
        Row(12348, 1522713600, "scroll", 104, "medicine", true),
        Row(12346, 1423612800, "click", 105, "medicine", false),
        Row(12348, 1337644800, "scroll", 106, "hitech", true),
        Row(12346, 1026000000, "click", 107, "medicine", false),
        Row(12346, 1332460800, "move", 108, "sport", false),
        Row(12345, 1276473600, "visit", 109, "hitech", true),
        Row(12347, 1289347200, "click", 110, "medicine", true),
        Row(12345, 985910400, "move", 111, "sport", true),
        Row(12349, 1542153600, "scroll", 112, "medicine", false),
        Row(12349, 1557100800, "click", 113, "hitech", false),
        Row(12345, 1547424000, "move", 114, "sport", true),
        Row(12348, 1489968000, "click", 115, "politics", true),
        Row(12348, 1366502400, "move", 116, "sport", true),
        Row(12345, 1297036800, "scroll", 117, "hitech", true),
        Row(12348, 1137715200, "move", 118, "medicine", true),
        Row(12348, 1495324800, "visit", 119, "hitech", true),
        Row(12349, 1359936000, "click", 120, "politics", false),
        Row(12347, 1125532800, "move", 121, "medicine", true),
        Row(12345, 1015372800, "scroll", 122, "sport", true),
        Row(12346, 1371772800, "scroll", 123, "politics", false),
        Row(12350, 1105660800, "scroll", 124, "politics", true),
        Row(12346, 1456617600, "scroll", 125, "hitech", false),
        Row(12346, 1268784000, "scroll", 126, "medicine", false),
        Row(12347, 1371600000, "visit", 127, "medicine", true),
        Row(12349, 1033171200, "move", 128, "hitech", false),
        Row(12349, 1171670400, "scroll", 129, "hitech", false)
    )

    val data2 = Seq(Row(1, 12345,"???????????????? ?????????? ????????????????????????", 1438732800),
        Row(1, 12346,"?????????????? ???????????????? ??????????????????????", 1631732800),
        Row(1, 12347,"???????????????? ???????????????? ????????????????????", 1334732800),
        Row(1, 12348,"???????????????????? ???????????? ??????????????????", 1238324800),
        Row(1, 12349,"?????????????????? ?????????? ????????????????", 1428732800),
        Row(1, 12350,"?????????????? ???????? ????????????????????", 1528632800)
    )

    var df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    import org.apache.spark.sql.functions._
    //?????????????? ??????-5 ?????????? ???????????????? ?????????????????????? ??????????
   df.groupBy("id").count()
     .sort(col("count").desc)
     .select("id")
     .show(5)

    // ?????????????????? ?????????????? ??????????????????????, ?? ?????????????? ???????? ????
   val sign_count = df.select("id").where(col("sign")===true).select(countDistinct("id")).collect()(0)(0).toString().toInt
   val user_count = df.select("id").select(countDistinct("id")).collect()(0)(0).toString().toInt
   println("?????????????? ??????????????????????, ?? ?????????????? ???????? ????: %s %%".format(100 * sign_count / user_count))

    //?????????????? ??????-5 ?????????????? ?????????? ???? ???????????????????? ???????????? ??????-???? ???????????? ???? ???????????? ????????????????

    //???????????????? ?????????????? ?? ???????????? ???????????? ???? ?????????????????? ???????????????????? ?????????????????? ?? ???????????? ?????????? ?? ???????????????? ???????? ??? 4 ????????(0-4, 4-8, 8-12 ?? ??.??.)

    //???????????????? ?????????????????? ???????????????????? ???? ???????????? ?????????????????????? ??????????????, ?? ?????????????? ???????????????? ???????? ???????????? ?????????? ?????????????????????? ???? ??????????.

    //???????????????? ???????????? ?????????? ????????????, ?????????????? ?????????? ?????????????????? ???????????????????? ?? ???? ???????????????????? ??????????
    var dfus = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema2)

    //?????????????? ?????????????? ??????????????????????, ?????????????? ???????????? ???????? ???? ???????? ?????????????? ?????? ??????????.
    df.join(dfus, df("id") === dfus("user_id"), "inner")
      .filter(df("tag")==="sport").select("fio").distinct()
      .show(false)

//    df.toDF("seq").select(
//        from_unixtime("timestamp", "MM-dd-yyyy").as("date_1")
//    ).show(false);
}
