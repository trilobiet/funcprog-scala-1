package timeusage

import org.apache.spark.sql._
import org.apache.spark.sql.types._

/** Main class */
object TimeUsage extends TimeUsageInterface {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .master("local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
    spark.close()
  }

  def timeUsageByLifePeriod(): Unit = {
    val (columns, initDf) = read("src/main/resources/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    // val finalDf = timeUsageGrouped(summaryDf)

    // summary
    val tust = timeUsageSummaryTyped(summaryDf)
    println("timeUsageSummaryTyped ===========================================")
    tust.show()
    tust.printSchema()
    println("timeUsageGroupedSql ===========================================")
    val finalDf = timeUsageGroupedSql(summaryDf)
    finalDf.show()
    println("timeUsageGroupedTyped ===========================================")
    val tugt = timeUsageGroupedTyped(tust)
    tugt.show()
  }

  /** @return The read DataFrame along with its column names. */
  def read(path: String): (List[String], DataFrame) = {
    val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(path)
    (df.schema.fields.map(_.name).toList, df)
  }

  /** @return An RDD Row compatible with the schema produced by `dfSchema`
    * @param line Raw fields
    */
  def row(line: List[String]): Row = {
    // The first column is a String, whereas the remaining columns have type Double.
    val colFirst = line.head
    val colsRest = line.tail.map(c => c.toDouble)
    Row.fromSeq(colFirst :: colsRest)
  }

  /** @return The initial data frame columns partitioned in three groups: primary needs (sleeping, eating, etc.),
    *         work and other (leisure activities)
    *
    * @see https://www.kaggle.com/bls/american-time-use-survey
    *
    * The dataset contains the daily time (in minutes) people spent in various activities. For instance, the column
    * “t010101” contains the time spent sleeping, the column “t110101” contains the time spent eating and drinking, etc.
    *
    * This method groups related columns together:
    * 1. “primary needs” activities (sleeping, eating, etc.). These are the columns starting with “t01”, “t03”, “t11”,
    *    “t1801” and “t1803”.
    * 2. working activities. These are the columns starting with “t05” and “t1805”.
    * 3. other activities (leisure). These are the columns starting with “t02”, “t04”, “t06”, “t07”, “t08”, “t09”,
    *    “t10”, “t12”, “t13”, “t14”, “t15”, “t16” and “t18” (those which are not part of the previous groups only).
    */
  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {

    val primaryNeeds =
      columnNames.filter( name => List("t01","t03","t11","t1801","t1803").exists( p => name.startsWith(p) ) )
    val workingActivities =
      columnNames.filter( name => List("t05","t1805").exists( p => name.startsWith(p) ) )
    val otherActivities =
      columnNames.filter( name => List("t02","t04","t06","t07","t08","t09","t10","t12","t13","t14","t15","t16","t18")
        .exists( p => name.startsWith(p) )
      ).filterNot( name => (primaryNeeds::workingActivities).contains(name) )

    val primaryNeedsCols = primaryNeeds.map( name => new Column(name) )
    val workingActivitiesCols = workingActivities.map( name => new Column(name) )
    val otherActivitiesCols = otherActivities.map( name => new Column(name) )
    val cols = (primaryNeedsCols, workingActivitiesCols, otherActivitiesCols)
    println(cols)
    cols
  }

  /** @return a projection of the initial DataFrame such that all columns containing hours spent on primary needs
    *         are summed together in a single column (and same for work and leisure). The “teage” column is also
    *         projected to three values: "young", "active", "elder".
    *
    * @param primaryNeedsColumns List of columns containing time spent on “primary needs”
    * @param workColumns List of columns containing time spent working
    * @param otherColumns List of columns containing time spent doing other activities
    * @param df DataFrame whose schema matches the given column lists
    *
    * This methods builds an intermediate DataFrame that sums up all the columns of each group of activity into
    * a single column.
    *
    * The resulting DataFrame should have the following columns:
    * - working: value computed from the “telfs” column of the given DataFrame:
    *   - "working" if 1 <= telfs < 3
    *   - "not working" otherwise
    * - sex: value computed from the “tesex” column of the given DataFrame:
    *   - "male" if tesex = 1, "female" otherwise
    * - age: value computed from the “teage” column of the given DataFrame:
    *   - "young" if 15 <= teage <= 22,
    *   - "active" if 23 <= teage <= 55,
    *   - "elder" otherwise
    * - primaryNeeds: sum of all the `primaryNeedsColumns`, in hours
    * - work: sum of all the `workColumns`, in hours
    * - other: sum of all the `otherColumns`, in hours
    *
    * Finally, the resulting DataFrame should exclude people that are not employable (ie telfs = 5).
    *
    * Note that the initial DataFrame contains time in ''minutes''. You have to convert it into ''hours''.
    */
  def timeUsageSummary(
    primaryNeedsColumns: List[Column],
    workColumns: List[Column],
    otherColumns: List[Column],
    df: DataFrame
  ): DataFrame = {
    // Transform the data from the initial dataset into data that make
    // more sense for our use case
    // Hint: you can use the `when` and `otherwise` Spark functions
    // Hint: don’t forget to give your columns the expected name with the `as` method
    // acdhirr sez: See https://stackoverflow.com/questions/39490236/difference-between-and-in-scala-spark
    // for the === operator
    val workingStatusProjection: Column =
      when($"telfs" >= 1 && $"telfs" < 3, "working" )   // $"telfs" <=> col("telfs")
      .otherwise("not working")
      .as("working")
    val sexProjection: Column =
      when($"tesex" === 1, "male" )
      .otherwise("female")
      .as("sex")
    val ageProjection: Column =
      when($"teage" >= 15 && $"teage" <= 22, "young" )
      .when($"teage" >= 23 && $"teage" <= 55, "active" )
      .otherwise("elder")
      .as("age")

    // Create columns that sum columns of the initial dataset
    // Hint: you want to create a complex column expression that sums other columns
    //       by using the `+` operator between them
    // Hint: don’t forget to convert the value to hours

    // val g: Int = primaryNeedsColumns.aggregate(0)((acc, col) => acc+col.as[Int], _+_)

    def toHour(c: Column): Column = c/60
    val primaryNeedsProjection: Column = expr(primaryNeedsColumns.map( toHour ).mkString("+")).as("primaryNeeds")
    val workProjection: Column = expr(workColumns.map( toHour ).mkString("+")).as("work")
    val otherProjection: Column = expr(otherColumns.map( toHour ).mkString("+")).as("other")
    val r = df
      .select(workingStatusProjection, sexProjection, ageProjection, primaryNeedsProjection, workProjection, otherProjection)
      .where($"telfs" <= 4) // Discard people who are not in labor force

    // r.show()
    r
  }

  /** @return the average daily time (in hours) spent in primary needs, working or leisure, grouped by the different
    *         ages of life (young, active or elder), sex and working status.
    * @param summed DataFrame returned by `timeUsageSumByClass`
    *
    * The resulting DataFrame should have the following columns:
    * - working: the “working” column of the `summed` DataFrame,
    * - sex: the “sex” column of the `summed` DataFrame,
    * - age: the “age” column of the `summed` DataFrame,
    * - primaryNeeds: the average value of the “primaryNeeds” columns of all the people that have the same working
    *   status, sex and age, rounded with a scale of 1 (using the `round` function),
    * - work: the average value of the “work” columns of all the people that have the same working status, sex
    *   and age, rounded with a scale of 1 (using the `round` function),
    * - other: the average value of the “other” columns all the people that have the same working status, sex and
    *   age, rounded with a scale of 1 (using the `round` function).
    *
    * Finally, the resulting DataFrame should be sorted by working status, sex and age.
    */
  def timeUsageGrouped(summed: DataFrame): DataFrame = {

    val sg: RelationalGroupedDataset = summed.groupBy($"working",$"sex",$"age")

    val ag = sg.agg(
        round(avg("primaryNeeds"),1).as("primaryNeeds"),
        round(avg("work"),1).as("work"),
        round(avg("other"),1).as("other")
    )
    .orderBy($"working",$"sex",$"age")

    // ag.show()
    ag
  }

  /**
    * @return Same as `timeUsageGrouped`, but using a plain SQL query instead
    * @param summed DataFrame returned by `timeUsageSumByClass`
    */
  def timeUsageGroupedSql(summed: DataFrame): DataFrame = {
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(timeUsageGroupedSqlQuery(viewName))
  }

  /** @return SQL query equivalent to the transformation implemented in `timeUsageGrouped`
    * @param viewName Name of the SQL view to use
    */
  def timeUsageGroupedSqlQuery(viewName: String): String =
    // note the 's' prefix below to evaluate the $viewName variable!
    s"""
      select
        working, sex, age,
        round(avg(primaryNeeds),1) as primaryNeeds,
        round(avg(work),1) as work,
        round(avg(other),1) as other
      from
        $viewName
      group by
        working, sex, age
      order by
        working, sex, age
    """


  /**
    * @return A `Dataset[TimeUsageRow]` from the “untyped” `DataFrame`
    * @param timeUsageSummaryDf `DataFrame` returned by the `timeUsageSummary` method
    *
    * Hint: you should use the `getAs` method of `Row` to look up columns and
    * cast them at the same time.
    */
  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] = {

    // map (untyped) row to TimeUsageRow class
    timeUsageSummaryDf.map(row => TimeUsageRow(
      row.getAs[String](0),
      row.getAs[String](1),
      row.getAs[String](2),
      row.getAs[Double](3),
      row.getAs[Double](4),
      row.getAs[Double](5)
    ))
  }


  /**
    * @return Same as `timeUsageGrouped`, but using the typed API when possible
    * @param summed Dataset returned by the `timeUsageSummaryTyped` method
    *
    * Note that, though they have the same type (`Dataset[TimeUsageRow]`), the input
    * dataset contains one element per respondent, whereas the resulting dataset
    * contains one element per group (whose time spent on each activity kind has
    * been aggregated).
    *
    * Hint: you should use the `groupByKey` and `typed.avg` methods.
    */
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {
    import org.apache.spark.sql.expressions.scalalang.typed

    // step 1: group by working, sex, age
    val t: KeyValueGroupedDataset[(String, String, String), TimeUsageRow]
      = summed.groupByKey(row => (row.working, row.sex, row.age))

    // step 2: aggregate (rounded) averages
    val t2: Dataset[((String, String, String), Double, Double, Double)] = t.agg(
      round(typed.avg[TimeUsageRow](row=>row.primaryNeeds),1).as[Double],
      round(typed.avg[TimeUsageRow](row=>row.work),1).as[Double],
      round(typed.avg[TimeUsageRow](row=>row.other),1).as[Double]
    )

    // map to TimeUsageRow (again)
    val t3 = t2.map{
      case((working,sex,age),primaryNeeds,work,other) =>
      TimeUsageRow(working,sex,age,primaryNeeds,work,other)
    }

    // return sorted results
    t3.sort($"working", $"sex", $"age")
  }
}

/**
  * Models a row of the summarized data set
  * @param working Working status (either "working" or "not working")
  * @param sex Sex (either "male" or "female")
  * @param age Age (either "young", "active" or "elder")
  * @param primaryNeeds Number of daily hours spent on primary needs
  * @param work Number of daily hours spent on work
  * @param other Number of daily hours spent on other activities
  */
case class TimeUsageRow(
  working: String,
  sex: String,
  age: String,
  primaryNeeds: Double,
  work: Double,
  other: Double
)
