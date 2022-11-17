import org.apache.spark.sql
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.functions.lit

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


    val data = Seq(Row(12345, 1667627426, "click", 101, "Sport", false),
      Row(12345, 1667627426, "click", 101, "Sport", false)
    )

    var df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.show()

}
