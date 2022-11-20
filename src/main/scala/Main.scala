import org.apache.spark.sql
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.functions.{col, desc, lit}

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


    val data = Seq(Row(12345, 1667627426, "click", 101, "Sport", false),
        Row(12345, 1438732800, "scroll", 101, "sport", false),
        Row(12345, 1380412800, "move", 102, "medicine", false),
        Row(12348, 1211760000, "visit", 103, "hitech", false),
        Row(12348, 1522713600, "scroll", 104, "medicine", true),
        Row(12346, 1423612800, "click", 105, "medicine", false),
        Row(12348, 1337644800, "scroll", 106, "hitech", true),
        Row(12346, 1026000000, "click", 107, "medicine", false),
        Row(12346, 1332460800, "move", 108, "sport", true),
        Row(12345, 1276473600, "visit", 109, "hitech", true),
        Row(12347, 1289347200, "click", 110, "medicine", false),
        Row(12345, 985910400, "move", 111, "sport", true),
        Row(12349, 1542153600, "scroll", 112, "medicine", false),
        Row(12349, 1557100800, "click", 113, "hitech", false),
        Row(12345, 1547424000, "move", 114, "sport", false),
        Row(12348, 1489968000, "click", 115, "politics", false),
        Row(12348, 1366502400, "move", 116, "sport", false),
        Row(12345, 1297036800, "scroll", 117, "hitech", true),
        Row(12348, 1137715200, "move", 118, "medicine", true),
        Row(12348, 1495324800, "visit", 119, "hitech", false),
        Row(12349, 1359936000, "click", 120, "politics", false),
        Row(12347, 1125532800, "move", 121, "medicine", false),
        Row(12345, 1015372800, "scroll", 122, "sport", false),
        Row(12346, 1371772800, "scroll", 123, "politics", true),
        Row(12350, 1105660800, "scroll", 124, "politics", true),
        Row(12346, 1456617600, "scroll", 125, "hitech", true),
        Row(12346, 1268784000, "scroll", 126, "medicine", false),
        Row(12347, 1371600000, "visit", 127, "medicine", false),
        Row(12349, 1033171200, "move", 128, "hitech", true),
        Row(12349, 1171670400, "scroll", 129, "hitech", false)
    )

    val data2 = Seq(Row(1, 12345,"Алексеев Борис Владимирович", 1438732800),
        Row(1, 12346,"Борисов Владимир Геннадьевич", 1631732800),
        Row(1, 12347,"Воробьев Геннадий Дмитриевич", 1334732800),
        Row(1, 12348,"Евстифеева Галина Андреевна", 1238324800),
        Row(1, 12349,"Хабибулин Марат Адамович", 1428732800),
        Row(1, 12350,"Зайцева Анна Михайловна", 1528632800)
    )

    var df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    //Вывести топ-5 самых активных посетителей сайта
    df.groupBy("id").count().sort(col("count").desc).show(5)

    // Посчитать процент посетителей, у которых есть ЛК

    //Вывести топ-5 страниц сайта по показателю общего кол-ва кликов на данной странице

    //Добавьте столбец к фрейму данных со значением временного диапазона в рамках суток с размером окна – 4 часа(0-4, 4-8, 8-12 и т.д.)

    //Выведите временной промежуток на основе предыдущего задания, в течение которого было больше всего активностей на сайте.

    //Создайте второй фрейм данных, который будет содержать информацию о ЛК посетителя сайта
    var dfus = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema2)

    //Вывести фамилии посетителей, которые читали хотя бы одну новость про спорт.
    df.join(dfus, df("id") === dfus("user_id"), "inner")
      .filter(df("tag")==="sport").select("fio").distinct()
      .show(false)
}
