import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession, functions, types}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, max, udf, _}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.immutable.ListMap

object Fifa {
  def main(args: Array[String]): Unit = {

    // creating Spark Session
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("fifa")
      .getOrCreate()


    //Loading the data
    val df = spark
      .read
      .option("header","true")
      .csv("data/data.csv")
      .withColumnRenamed("Preferred Foot","Preferred_Foot")

    spark.sparkContext.setLogLevel("ERROR")

    val df1_cast = df
      .selectExpr("Name",
        "Club",
        "Nationality",
        "Preferred_Foot",
        "Joined",
        "cast(Age as int) Age",
        "cast(Overall as int) Overall",
        "Value",
        "Wage",
        "Position",
        "cast(Crossing as int) Crossing",
        "cast(Finishing as int) Finishing",
        "cast(HeadingAccuracy as int) HeadingAccuracy",
        "cast(ShortPassing as int) ShortPassing",
        "cast(Volleys as int) Volleys",
        "cast(Dribbling as int) Dribbling",
        "cast(Curve as int) Curve",
        "cast(FKAccuracy as int) FKAccuracy",
        "cast(LongPassing as int) LongPassing",
        "cast(BallControl as int) BallControl",
        "cast(Acceleration as int) Acceleration",
        "cast(SprintSpeed as int) SprintSpeed",
        "cast(Agility as int) Agility",
        "cast(Reactions as int) Reactions",
        "cast(Balance as int) Balance",
        "cast(ShotPower as int) ShotPower",
        "cast(Jumping as int) Jumping",
        "cast(Stamina as int) Stamina",
        "cast(Strength as int) Strength",
        "cast(LongShots as int) LongShots",
        "cast(Aggression as int) Aggression",
        "cast(Interceptions as int) Interceptions",
        "cast(Positioning as int) Positioning",
        "cast(Vision as int) Vision",
        "cast(Penalties as int) Penalties",
        "cast(Composure as int) Composure",
        "cast(Marking as int) Marking",
        "cast(StandingTackle as int) StandingTackle",
        "cast(SlidingTackle as int) SlidingTackle",
        "cast(GKDiving as int) GKDiving",
        "cast(GKHandling as int) GKHandling",
        "cast(GKKicking as int) GKKicking",
        "cast(GKPositioning as int) GKPositioning",
        "cast(GKReflexes as int) GKReflexes")


    //persisting the dataframe
    df1_cast.persist()

    val L1 = List("RWM","RM","RCM","CM","CAM","CDM","LCM","LM","LWM")
    val L2 = List("RB","CB","RCB","LCB","LB")
    val L3 = List("RF","CF","LF","ST")


    //(a) finding the club has the most number of left footed midfielders under 30 years of age


    val most_no_of_Left_midfielder =df1_cast
      .select("Age","Preferred_Foot","Position","Club")
      .filter(df1_cast("Age") <30 && df1_cast("Preferred_Foot") ==="Left" && df1_cast("Position").isin(L1:_*))
      .groupBy("Club")
      .count()
      .select("Club","count")
      .where(col("Club").isNotNull)


    val most_no_of_Left_midfielder1 = most_no_of_Left_midfielder
      .withColumn("rank", dense_rank().over(Window.orderBy(col("count").desc)))
      .where(col("rank")===1)
      .select(col("Club") as "Club_with_most_number_of_left_footed_midfielders_under_30")
      .show()


    // (b) finding strongest team by overall rating for a 4-4-2 formation

    val df2 = df1_cast
      .select("Name","Club","Position","Overall")
      .withColumn("rank", dense_rank().over(Window.partitionBy("Position").orderBy(col("Overall").desc)))
      .where(col("rank")===1)

    //persisting df2
    df2.persist()

    val rank_list1 =List(1,2,3,4)
    val rank_list2= List(1,2)

    val L1_df = df2
      .filter(col("Position").isin(L1:_*))
      .withColumn("rank", row_number().over(Window.orderBy(col("Overall").desc)))
      .where(col("rank").isin(rank_list1:_*))

    val L2_df = df2.filter(col("Position").isin(L2:_*))
      .withColumn("rank", row_number().over(Window.orderBy(col("Overall").desc)))
      .where(col("rank").isin(rank_list1:_*))

    val L3_df = df2.filter(col("Position").isin(L3:_*))
      .withColumn("rank", row_number().over(Window.orderBy(col("Overall").desc)))
      .where(col("rank").isin(rank_list2:_*))

    val L4_df = df2.filter(col("Position") ==="GK")

    L1_df
      .union(L2_df)
      .union(L3_df)
      .union(L4_df)
      .drop("rank")
      .withColumnRenamed("Name","4-4-2 formation players list")
      .show()

    // As there are no furthermore operations with df2,hence unpersisting the dataframe
    df2.unpersist()

    // (c) finding the most expensive squad value in the world


    val cleaned_df = df1_cast
      .withColumn("Value",
        when(col("Value").endsWith("M"),regexp_extract(col("Value"),"\\d+",0)*1000000)
          .when(col("Value").endsWith("K"),regexp_extract(col("Value"),"\\d+",0)*1000))
      .withColumn("Wage",when(col("Wage").endsWith("K"),regexp_extract(col("Wage"),"\\d+",0)*1000)
        .when(col("Wage").endsWith("M"),regexp_extract(col("Wage"),"\\d+",0)*1000000))
      .withColumn("Value",col("Value").cast(IntegerType))
      .withColumn("Wage",col("Wage").cast(IntegerType))

    // persisting cleaned_df dataframe
    cleaned_df.persist()

    cleaned_df
      .groupBy("Club")
      .agg(sum("Value").alias("cummulative_Value"))
      .withColumn("rank", dense_rank().over(Window.orderBy(col("cummulative_Value").desc)))
      .where(col("rank")===1)
      .select(col("Club") as "Most_expensive_squad")
      .show()


    cleaned_df
      .groupBy("Club")
      .agg(sum("Wage").alias("cummulative_Wage"))
      .withColumn("rank", dense_rank().over(Window.orderBy(col("cummulative_Wage").desc)))
      .where(col("rank")===1)
      .select(col("Club") as "Squad_with_largest_wage_bill")
      .show()


    // (d) The position pays the highest wage in average

    cleaned_df
      .select("Position","Wage")
      .groupBy("Position")
      .agg(avg("Wage").alias("avg_wage"))
      .withColumn("rank",dense_rank().over(Window.orderBy(col("avg_wage").desc)))
      .where(col("rank")===1)
      .select(col("Position") as ("Highest_Paid_Position") )
      .show()

    //unpersisting cleaned_df as it is not required for future computations
    cleaned_df.unpersist()

    // (e) four attributes which are most relevant to becoming a good goalkeeper

    val df_attribute =df1_cast
      .select(
        "Crossing",
        "Finishing",
        "Overall",
        "HeadingAccuracy",
        "ShortPassing",
        "Volleys",
        "Dribbling",
        "Curve",
        "FKAccuracy",
        "LongPassing",
        "BallControl",
        "Acceleration",
        "SprintSpeed",
        "Agility",
        "Reactions",
        "Balance",
        "ShotPower",
        "Jumping",
        "Stamina",
        "Strength",
        "LongShots",
        "Aggression",
        "Interceptions",
        "Positioning",
        "Vision",
        "Penalties",
        "Composure",
        "Marking",
        "StandingTackle",
        "SlidingTackle",
        "GKDiving",
        "GKHandling",
        "GKKicking",
        "GKPositioning",
        "GKReflexes")

    //persisting df_attribute
    df_attribute.persist()

    df_attribute
      .filter(col("Position") === "GK")
      .withColumn("rank", row_number().over(Window.orderBy(col("Overall").desc)))
      .where(col("rank") === 1)


    val x= df_attribute.collectAsList().get(0)
    val y =df_attribute.columns
    var A:Map[String,Int] = Map()


    for (i<- 0 to y.length-1)
    {
      A +=(y(i) -> x.getInt(i))
    }

    val sorted_attributes = ListMap(A.toSeq.sortWith(_._2 > _._2):_*)

    val List_attributes= sorted_attributes.keys.toList

    print("The top 4 attributes which are most relevant to becoming a good goalkeeper are: ")

    for(i<-0 to 3)
    {
      println(List_attributes(i))
    }

    // (f) 5 attributes which are most relevant to becoming a top striker

    val df_attribute1 =df_attribute
      .filter(col("Position") === "ST")
      .withColumn("rank", row_number().over(Window.orderBy(col("Overall").desc)))
      .where(col("rank") === 1)


    val x1= df_attribute1.collectAsList().get(0)
    val y1 =df_attribute1.columns
    var A1:Map[String,Int] = Map()


    for (i<- 0 to y1.length-1)
    {
      A1 +=(y1(i) -> x1.getInt(i))
    }

    val sorted_attributes1 = ListMap(A1.toSeq.sortWith(_._2 > _._2):_*)

    val List_attributes1= sorted_attributes1.keys.toList

    print("The top 5 attributes which are most relevant to becoming a good goalkeeper are: ")

    for(i<-0 to 4)
    {
      println(List_attributes1(i))
    }

    // unpersisting df_attribute
    df_attribute.unpersist()

    // proble number 3

    val postgres_df = cleaned_df.select("Overall",
    "Position",
      "Nationality",
      "Name",
      "Club",
      "Wage",
      "Value",
      "Joined",
      "Age")

    val final_df = postgres_df
        .withColumnRenamed("Overall","Overall Rating")
        .withColumnRenamed("Club","Club Name")
      .withColumnRenamed("Nationality","Country")
      .withColumn("Wage",col("Wage").cast(DoubleType))
      .withColumn("Value",col("Value").cast(DoubleType))

    final_df.printSchema()

    val props = new Properties()
    props.put("user", "postgres")
    props.put("password", "postgres")
    props.put("driver", "org.postgresql.Driver")

    final_df
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url = "jdbc:postgresql://localhost:5555/fifa", table = "fifa19", connectionProperties = props)


    spark.stop()
  }
}