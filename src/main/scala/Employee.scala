package org.example

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.current_date
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SaveMode, SparkSession}

object Employee {

  case class employeeStruct(emp_id: Int,
                            emp_name: String,
                            job_name: String,
                            manager_id: String,
                            hire_date: String,
                            salary: String,
                            commission: String,
                            dep_id: String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Employee")
      .master("local[*]")
      .getOrCreate()

    val employees = spark.sparkContext.textFile("data/EMP.txt")

    val employeesWithIndex = employees.zipWithIndex()

    // 10) Remove the first line in text file.
    // First Approach
    val firstLine = employees.first()
    val employeesWithoutFirstLine = employees.filter(row => row != firstLine)

    // Second Approach
    val employeesWithIndexApproach = employeesWithIndex.filter(_._2 > 0)

    // 11)	Process the above txt file using RDD and get the highest salary per employee id?
    val employees_1 = employeesWithIndex.filter(_._2 > 1)
    val empSalaries = employees_1.map(emp => emp._1.split(",")).map(emp_sal => emp_sal(5).toInt)

    // First Approach
    println("Max Salary: " + empSalaries.max())

    // Second Approach
    val max_salaries = empSalaries.distinct().sortBy(x => x, false, 1)
    max_salaries.take(1).foreach(x => println("Max Salary: " + x))


    // 12)	Calculate second highest salary?
    val second_highest_salary = max_salaries.zipWithIndex.filter(x => x._2 == 1)
    second_highest_salary.take(1).foreach(x => println("Second Highest Salary: " + x._1))

    // 13)	Remove the duplicates?
    val distinctEmp = empSalaries.distinct();
    println("Distinct Salaries: ")
    distinctEmp.foreach(x => println(x))

    // 14)	Convert above text file RDD to Data Frame and add extra column system date (Current Date)?
    val empRDD = employees_1
      .map(emp => emp._1.split(","))

    import spark.implicits._
    val empDF = empRDD.map {
      case Array(emp_id, emp_name, job_name, manager_id,
      hire_date, salary, commission, dep_id)
      => employeeStruct(emp_id.toInt, emp_name,
        job_name, manager_id,
        hire_date, salary,
        commission, dep_id)
    }.toDF().withColumn("system date", current_date())

    empDF.printSchema()
    empDF.show(false)

    //    15)	Change the employee id data type to string
    val empDF_Int_ID = empDF.withColumn("emp_id", empDF("emp_id").cast(StringType))

    empDF_Int_ID.printSchema()

    //    16)	Generate csv file from above data frame with name as EMP_SPARK.csv
    empDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .csv("data/EMP_SPARK")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path("data/EMP_SPARK/part*"))(0).getPath.getName
    println(file)
    fs.rename(new Path("data/EMP_SPARK/" + file), new Path("data/EMP_SPARK.csv"))

  }

}
