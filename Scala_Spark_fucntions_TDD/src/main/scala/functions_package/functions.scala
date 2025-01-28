package functions_package

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

object functions {
  
  
  /*******************************************************************************************************************
   Creates a DataFrame from a sequence of tuples containing information about students (name, age, grade). Displays the
   schema of the DataFrame, filters students with a grade higher than 8, selects their names, and sorts them in
   descending order by grade.

   * @param students A DataFrame containing student information, including columns such as "name", "age", and "grade".
   * @param spark    The active SparkSession required to perform the transformations.
   * @return A DataFrame containing the names of students with grades higher than 8, sorted in descending order by grade.
   *******************************************************************************************************************/

  def filterAndSortStudentsByGrade(students: DataFrame)(spark:SparkSession): DataFrame = {

    students.printSchema()

    students
      .filter(col("grade")>8)
      .orderBy(col("grade").desc)
      .select("name")

  }



  /*******************************************************************************************************************
   Define a function that determines whether a number is even or odd.
   Apply this function to a column in a DataFrame containing a list of numbers.

   * @param numbers A DataFrame containing a single column of integers to classify as "even" or "odd".
   * @param spark   The active SparkSession needed to register and apply the UDF.
   * @return A DataFrame with an additional column named "Classification" that labels each number as "even" or "odd".
   ********************************************************************************************************************/

  def classifyNumbersAsEvenOrOdd(numbers: DataFrame)(spark:SparkSession): DataFrame =  {

    def evenOrOdd(numbers: Integer): String = if(numbers%2==0) "even" else "odd"

    val classifyNumbers = udf((numbers: Integer) => evenOrOdd(numbers))

    numbers.withColumn("Classification" , classifyNumbers(col(numbers.columns(0))))

  }



  /*******************************************************************************************************************
   Given two DataFrames:
   - One containing student information (id, name)
   - Another containing grades (id, subject, grade),
   performs a join between them and calculates the average grade per student.

   * @param students       A DataFrame containing student information with columns: "id" and "name".
   * @param grades         A DataFrame containing grades information with columns: "id", "subject", and
   *                       "grade".
   * @return A DataFrame containing each student's "id", "name", and their average grade rounded to two decimal places
   *         , labeled as "average grade".
   ********************************************************************************************************************/

  def calculateAverageGradePerStudent(students: DataFrame , grades: DataFrame): DataFrame = {


    students
      .join(grades, Seq(students.columns(0), grades.columns(0)) , "inner")
      .groupBy("id","name").agg(avg("grade"))
      .withColumnRenamed("avg(grade)", "average grade")
      .withColumn("average grade", round(col("average grade"),2))


  }



  /*******************************************************************************************************************
   Creates an RDD from a list of words and counts the occurrences of each word.

   * @param words A list of words to process.
   * @param spark The active SparkSession needed for parallelizing the data and performing transformations.
   * @return An RDD of tuples, where each tuple contains a word and its corresponding count of occurrences in the list.
   ********************************************************************************************************************/

  def countWordOccurrences(words: List[String])(spark:SparkSession): RDD[(String, Int)] = {

    spark.sparkContext.parallelize(words.toSeq)
      .map(word  => (word , words.count(_==word )))
      .distinct()

  }



  /*******************************************************************************************************************
   Loads a CSV file containing sales information (id_venta, id_producto, cantidad, precio_unitario) and calculates the
   total revenue (quantity * unit_price) per product.

   * @param sales A DataFrame containing sales data with the columns "id_venta", "id_producto", "cantidad", and
   *              "precio_unitario".
   * @param spark The active SparkSession required to perform transformations and aggregations.
   * @return A DataFrame containing the total revenue per product, with columns "id_producto" and "ingreso total".
   ********************************************************************************************************************/

  def calculateTotalRevenuePerProduct(sales: DataFrame)(spark:SparkSession): DataFrame = {

    sales
      .groupBy("id_producto").agg(
        sum(
          col("cantidad")*col("precio_unitario")
        )
      )
      .withColumnRenamed("sum((cantidad * precio_unitario))", "ingreso total")
  }

}

