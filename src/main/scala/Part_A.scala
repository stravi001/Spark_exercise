import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import CsvFileToAvro.get_spark_session
import org.apache.spark.sql.catalyst.analysis.TempTableAlreadyExistsException
object Part_A extends App {

  def Task_1: Long = {

    val spark = get_spark_session

    try {
      spark.sqlContext.sql("create temporary view vault using avro options (path \"src/main/resources/vault.avro\")") //try, protoze jiz muze existovat
    }
    catch {
      case e: TempTableAlreadyExistsException => println("Temporary view 'vault' already exists")
    }

    val df_select_all = spark.sql("select * from vault")

    df_select_all.count

  }

  def Task_2_var_a: Long = {

    val spark = get_spark_session

    try {
      spark.sqlContext.sql("create temporary view vault using avro options (path \"src/main/resources/vault.avro\")")
    }
    catch {
      case e: TempTableAlreadyExistsException => println("Temporary view 'vault' already exists")
    }

    val df_unique_vaults = spark.sql("select distinct(vaultid) from vault")

    df_unique_vaults.count

  }

  def Task_2_var_b: Long = {

    val spark = get_spark_session

    try {
      spark.sqlContext.sql("create temporary view vault using avro options (path \"src/main/resources/vault.avro\")")
    }
    catch {
      case e: TempTableAlreadyExistsException => println("Temporary view 'vault' already exists")
    }

    val df_select_all = spark.sql("select * from vault")

    val ds_unique_vaults = df_select_all.dropDuplicates("vaultId")

    ds_unique_vaults.count

  }

  def Task_3_var_a: DataFrame = {

    val spark = get_spark_session

    try {
      spark.sqlContext.sql("create temporary view vault using avro options (path \"src/main/resources/vault.avro\")")
    }
    catch {
      case e: TempTableAlreadyExistsException => println("Temporary view 'vault' already exists")
    }

    val df_count_content = spark.sql(
      """select
        |vaultid as vaultId,
        |sum((length(content) - length(replace(lower(content), 'gold')))/ length('gold')) as numberOfGoldCoins,
        |sum((length(content) - length(replace(lower(content), 'silver')))/ length('silver')) as numberOfSilverCoins,
        |sum((length(content) - length(replace(lower(content), 'euro')))/ length('euro')) as numberOfEuroCoins
        |from vault
        |group by vaultid""".stripMargin)

    //println("Task 3 var a: How much gold, silver, euro is contained within every vault = ")

    df_count_content

  }

  def Task_3_var_b: DataFrame = {

    val spark = get_spark_session

    try {
      spark.sqlContext.sql("create temporary view vault using avro options (path \"src/main/resources/vault.avro\")")
    }
    catch {
      case e: TempTableAlreadyExistsException => println("Temporary view 'vault' already exists")
    }

    val df_select_all = spark.sql("select * from vault")

    val df_count_content = df_select_all
      .selectExpr(
        "vaultid",
        "(length(content) - length(replace(lower(content), 'gold'))) / length('gold') as numberOfGoldCoins",
        "(length(content) - length(replace(lower(content), 'silver')))/ length('silver') as numberOfSilverCoins",
        "(length(content) - length(replace(lower(content), 'euro')))/ length('euro') as numberOfEuroCoins")
      .groupBy("vaultId")
      .sum("numberOfGoldCoins", "numberOfSilverCoins", "numberOfEuroCoins")
      .toDF(colNames = "vaultId", "numberOfGoldCoins", "numberOfSilverCoins", "numberOfEuroCoins") //jinak sum pred zacatkem nazvu sloupcu

    //println("Task 3 var b: How much gold, silver, euro is contained within every vault = ")

    df_count_content

  }

  def Task_4: Double = {

    val spark = get_spark_session

    try {
      spark.sqlContext.sql("create temporary view vault using avro options (path \"src/main/resources/vault.avro\")")
    }
    catch {
      case e: TempTableAlreadyExistsException => println("Temporary view 'vault' already exists")
    }

    val df_select_all = spark.sql("select * from vault")

    val v_wealth_in_gold = df_select_all
       .selectExpr(
         """sum((length(content) - length(replace(lower(content), 'gold'))) / length('gold')
           |+ (length(content) - length(replace(lower(content), 'silver'))) / length('silver') / 10
           |+ (length(content) - length(replace(lower(content), 'euro'))) / length('euro') /5.2 / 10)
           |as wealthOfGoldCoins""".stripMargin)
       .first()

      v_wealth_in_gold.getDouble(0)

  }

  def Task_5_var_a: Double = {

    val spark = get_spark_session

    try {
      spark.sqlContext.sql("create temporary view vault using avro options (path \"src/main/resources/vault.avro\")")
    }
    catch {
      case e: TempTableAlreadyExistsException => println("Temporary view 'vault' already exists")
    }

    val df_select_all = spark.sql("select * from vault")

    val v_wealth_in_silver = df_select_all
      .selectExpr(
        """sum((length(content) - length(replace(lower(content), 'gold'))) / length('gold') * 10
          |+ (length(content) - length(replace(lower(content), 'silver'))) / length('silver')
          |+ (length(content) - length(replace(lower(content), 'euro'))) / length('euro') /5.2)
          |as wealthOfGoldCoins""".stripMargin)
      .first()

    v_wealth_in_silver.getDouble(0)

  }

  def Task_5_var_b: Double = {

    Task_4 * 10

  }

  def Task_6_var_a: Double = {

    val spark = get_spark_session

    try {
      spark.sqlContext.sql("create temporary view vault using avro options (path \"src/main/resources/vault.avro\")")
    }
    catch {
      case e: TempTableAlreadyExistsException => println("Temporary view 'vault' already exists")
    }

    val df_select_all = spark.sql("select * from vault")

    val v_wealth_in_silver = df_select_all
      .selectExpr(
        """sum((length(content) - length(replace(lower(content), 'gold'))) / length('gold') * 10 * 5.2
          |+ (length(content) - length(replace(lower(content), 'silver'))) / length('silver') * 5.2
          |+ (length(content) - length(replace(lower(content), 'euro'))) / length('euro'))
          |as wealthOfGoldCoins""".stripMargin)
      .first()

    v_wealth_in_silver.getDouble(0)

  }

  def Task_6_var_b: Double = {

    Task_4 * 10 * 5.2

  }

  def Task_6_var_c: Double = {

    Task_5_var_a * 5.2

  }

  def Task_7(inp_dataframe: DataFrame, inp_output_path: String): Unit = {

    inp_dataframe.write.format("avro").mode(SaveMode.Overwrite).save(inp_output_path)

  }

  println("Task 1: How many records does file contain = " + Task_1)
  println("Task 2 var a: How many vaults does scrooge have according to the csv = " + Task_2_var_a)
  println("Task 2 var b: How many vaults does scrooge have according to the csv = " + Task_2_var_b)
  Task_3_var_a.show
  Task_3_var_b.show
  println("Task 4: Calculate Scrooge's total wealth in gold = " + Task_4)
  println("Task 5 var a: Calculate Scrooge's total wealth in silver = " + Task_5_var_a)
  println("Task 5 var b: Calculate Scrooge's total wealth in silver = " + Task_5_var_b)
  println("Task 6 var a: Calculate Scrooge's total wealth in silver = " + Task_6_var_a)
  println("Task 6 var b: Calculate Scrooge's total wealth in silver = " + Task_6_var_b)
  println("Task 6 var c: Calculate Scrooge's total wealth in silver = " + Task_6_var_c)
  Task_7(Task_3_var_b, "./src/main/resources/vaults_content_cnt.avro")

  /*val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Spark")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  spark.sqlContext.sql("create temporary view vault using avro options (path \"src/main/resources/vault.avro\")")

  val df_select_all = spark.sql("select * from vault")

  //Task 1
  println("Task 1: How many records does file contain = " + Task_1)
  //End Task 1

  //Task 2
  val df_unique_vaults = spark.sql("select distinct(vaultid) from vault")
  println("Task 2: How many vaults does scrooge have according to the csv = " + df_unique_vaults.count)

  //or

  val ds_unique_vaults = df_select_all.dropDuplicates("vaultId")
  println("Task 2: How many vaults does scrooge have according to the csv = " + ds_unique_vaults.count)
  //End Task 2

  //Task 3
  val df_count_content = spark.sql(
    """select
      |vaultid as vaultId,
      |sum((length(content) - length(replace(lower(content), 'gold')))/ length('gold')) as numberOfGoldCoins,
      |sum((length(content) - length(replace(lower(content), 'silver')))/ length('silver')) as numberOfSilverCoins,
      |sum((length(content) - length(replace(lower(content), 'euro')))/ length('euro')) as numberOfEuroCoins
      |from vault
      |group by vaultid""".stripMargin)

  println("Task 3: How much gold, silver, euro is contained within every vault = ")
  df_count_content.show()

  //or

  val df_count_content2 = df_select_all
                            .selectExpr(
                        "vaultid",
                              "(length(content) - length(replace(lower(content), 'gold'))) / length('gold') as numberOfGoldCoins",
                              "(length(content) - length(replace(lower(content), 'silver')))/ length('silver') as numberOfSilverCoins",
                              "(length(content) - length(replace(lower(content), 'euro')))/ length('euro') as numberOfEuroCoins")
                            .groupBy("vaultId")
                            .sum("numberOfGoldCoins", "numberOfSilverCoins", "numberOfEuroCoins")
                            .toDF(colNames = "vaultId", "numberOfGoldCoins", "numberOfSilverCoins", "numberOfEuroCoins") //jinak sum pred zacatkem nazvu sloupcu

  println("Task 3: How much gold, silver, euro is contained within every vault = ")
  df_count_content2.show()
  //End Task 3

  //Task 4
  val v_wealth_in_gold =  df_count_content2.selectExpr("sum(numberOfGoldCoins + numberOfSilverCoins/10 + numberOfEuroCoins/5.2/10) as wealthOfGoldCoins")
                                            .first()

  println("Task 4: Calculate Scrooge's total wealth in gold = " + v_wealth_in_gold.getDouble(0))
  //End Task 4

  //Task 5
  val v_wealth_in_silver = df_count_content2.selectExpr("sum(numberOfGoldCoins*10 + numberOfSilverCoins + numberOfEuroCoins/5.2) as wealthOfGoldCoins")
                                            .first()

  println("Task 5: Calculate Scrooge's total wealth in silver = " + v_wealth_in_silver.getDouble(0))

  //or

  val v_wealth_in_silver2 = v_wealth_in_gold.getDouble(0) * 10

  println("Task 5: Calculate Scrooge's total wealth in silver = " + v_wealth_in_silver2)
  //End Task 5

  //Task 6
  val v_wealth_in_euro = df_count_content2.selectExpr("sum(numberOfGoldCoins*10*5.2 + numberOfSilverCoins*5.2 + numberOfEuroCoins) as wealthOfGoldCoins")
                                          .first()

  println("Task 6: Calculate Scrooge's total wealth in euro = " + v_wealth_in_euro.getDouble(0))

  //or

  val v_wealth_in_euro2 = v_wealth_in_gold.getDouble(0) * 52

  println("Task 6: Calculate Scrooge's total wealth in euro = " + v_wealth_in_euro2)
  //End Task 6

  //Task 7
  df_count_content2.write.format("avro").mode(SaveMode.Overwrite).save("./src/main/resources/vaults_content_cnt.avro")
  //End Task 7*/

}