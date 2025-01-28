package functions_package


import functions_package.functions.{filterAndSortStudentsByGrade, classifyNumbersAsEvenOrOdd, calculateAverageGradePerStudent, countWordOccurrences, calculateTotalRevenuePerProduct}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import utils.TestInit

class functionsTest extends TestInit{

  "filterAndSortStudentsByGrade" should "Show the schema of the DataFrame, names of students with grades > 8, in descending order." in{

    import spark.implicits._
    val studentsDf: DataFrame = spark.sparkContext.parallelize(
      Seq(
        ("Ana",     25, 8.5),
        ("Luis",    30, 9.0),
        ("Carlos",  28, 7.5),
        ("Marta",   22, 6.8),
        ("Javier",  35, 8.0),
        ("Laura",   29, 9.2),
        ("Sofía",   27, 7.9),
        ("Pedro",   32, 6.5),
        ("Lucía",   24, 8.3),
        ("Roberto", 31, 7.2)
      )
    ).toDF("name","age","grade")

    filterAndSortStudentsByGrade(studentsDf)(spark).show()

    filterAndSortStudentsByGrade(studentsDf)(spark)
      .head()
      .getAs("name")
      .toString shouldBe "Laura"

    filterAndSortStudentsByGrade(studentsDf)(spark).count() shouldBe 4

  }


  "classifyNumbersAsEvenOrOdd" should "Determine whether the numbers in a DataFrame are even or odd" in{

    import spark.implicits._
    val numbersDf: DataFrame = spark.sparkContext.parallelize(
      Seq(-45, 72, 89, -13, 0, 56, -92, 38, 17, -76)
    ).toDF("Number")

    classifyNumbersAsEvenOrOdd(numbersDf)(spark).show()

    classifyNumbersAsEvenOrOdd(numbersDf)(spark)
      .filter(col("Number")===17)
      .head()
      .getAs("Classification")
      .toString shouldBe "odd"

    classifyNumbersAsEvenOrOdd(numbersDf)(spark)
      .filter(col("Number")===72)
      .head()
      .getAs("Classification")
      .toString shouldBe "even"

  }


  "calculateAverageGradePerStudent" should "Return a DataFrame with the average grades of each student." in{

    import spark.implicits._
    val studentsDf: DataFrame = spark.sparkContext.parallelize(
      Seq(
        (1,  "Ana"),
        (2,  "Luis"),
        (3,  "Carlos"),
        (4,  "Marta"),
        (5,  "Javier")
      )
    ).toDF("id", "name")

    val gradesDf: DataFrame = spark.sparkContext.parallelize(
      Seq(
        (3,  "Matemáticas", 6.8),
        (3,  "Literatura",  7.3),
        (3,  "Física",      8.0),
        (1,  "Matemáticas", 9.2),
        (1,  "Literatura",  8.7),
        (1,  "Física",      7.8),
        (5,  "Matemáticas", 8.0),
        (5,  "Literatura",  6.7),
        (5,  "Física",      7.6),
        (4,  "Matemáticas", 9.5),
        (4,  "Literatura",  7.4),
        (4,  "Física",      8.1),
        (2,  "Matemáticas", 7.3),
        (2,  "Literatura",  9.0),
        (2,  "Física",      8.5)
      )
    ).toDF("id", "subject","grade")

    calculateAverageGradePerStudent(studentsDf,gradesDf).show()

    calculateAverageGradePerStudent(studentsDf,gradesDf)
      .filter(col("id")===2)
      .head()
      .getAs("average grade")
      .toString
      .toDouble shouldBe 8.27

  }


  "countWordOccurrences" should "Return how many times each word appears in a list" in{

    val wordsList: List[String] = List("manzana", "uva", "cereza", "manzana", "naranja", "melocotón", "uva", "uva")

    import spark.implicits._
    countWordOccurrences(wordsList)(spark).toDF().show()

    countWordOccurrences(wordsList)(spark)
      .filter(_._1=="manzana")
      .first() shouldBe("manzana",2)
  }


  "calculateTotalRevenuePerProduct" should "Return the total revenue per product" in{

    val ventas: DataFrame = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .csv("src/test/resources/ventas.csv")

    calculateTotalRevenuePerProduct(ventas)(spark).show()
    calculateTotalRevenuePerProduct(ventas)(spark)
      .filter(col("id_producto")===102)
      .head()
      .getAs("ingreso total")
      .toString
      .toDouble shouldBe 405.0
  }

}
