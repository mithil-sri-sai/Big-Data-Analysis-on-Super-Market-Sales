
//reading data from the dataset

val data=sc.textFile("supermarket_sales.csv")
//data: org.apache.spark.rdd.RDD[String] = supermarket_sales.csv MapPartitionsRDD[1] at textFile at <console>:24


val SalesRDD = data.map { l =>
  val s = l.split(',')
  val (invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods, tax, total, date, time, payment_type, gross_income, rating) = (s(0), s(1), s(2), s(3), s(4), s(5), s(6), s(7), s(8), s(9), s(10), s(11), s(12), s(13), s(14))
  (invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods, tax, total, date, time, payment_type, gross_income, rating)
}
//SalesRDD: org.apache.spark.rdd.RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = MapPartitionsRDD[2] at map at <console>:26

SalesRDD.toDF("invoice_id", "branch", "customer_type", "gender", "product", "unit_price", "quantity", "cost_of_goods","tax", "total", "date", "time", "payment_type", "gross_income", "rating").show()
/*
+-----------+------+-------------+------+--------------------+----------+--------+-------------+-------+--------+----------+-----+------------+------------+------+
| invoice_id|branch|customer_type|gender|             product|unit_price|quantity|cost_of_goods|    tax|   total|      date| time|payment_type|gross_income|rating|
+-----------+------+-------------+------+--------------------+----------+--------+-------------+-------+--------+----------+-----+------------+------------+------+
|750-67-8428|     A|       Member|Female|   Health and beauty|     74.69|       7|       522.83|26.1415|548.9715|01-05-2019|13:08|     Ewallet|     26.1415|   9.1|
|226-31-3081|     C|       Normal|Female|Electronic access...|     15.28|       5|         76.4|   3.82|   80.22|03-08-2019|10:29|        Cash|        3.82|   9.6|
|631-41-3108|     A|       Normal|  Male|  Home and lifestyle|     46.33|       7|       324.31|16.2155|340.5255|03-03-2019|13:23| Credit card|     16.2155|   7.4|
|123-19-1176|     A|       Member|  Male|   Health and beauty|     58.22|       8|       465.76| 23.288| 489.048|01-27-2019|20:33|     Ewallet|      23.288|   8.4|
|373-73-7910|     A|       Normal|  Male|   Sports and travel|     86.31|       7|       604.17|30.2085|634.3785|02-08-2019|10:37|     Ewallet|     30.2085|   5.3|
|699-14-3026|     C|       Normal|  Male|Electronic access...|     85.39|       7|       597.73|29.8865|627.6165|03-25-2019|18:30|     Ewallet|     29.8865|   4.1|
|355-53-5943|     A|       Member|Female|Electronic access...|     68.84|       6|       413.04| 20.652| 433.692|02-25-2019|14:36|     Ewallet|      20.652|   5.8|
|315-22-5665|     C|       Normal|Female|  Home and lifestyle|     73.56|      10|        735.6|  36.78|  772.38|02-24-2019|11:38|     Ewallet|       36.78|     8|
|665-32-9167|     A|       Member|Female|   Health and beauty|     36.26|       2|        72.52|  3.626|  76.146|01-10-2019|17:15| Credit card|       3.626|   7.2|
|692-92-5582|     B|       Member|Female|  Food and beverages|     54.84|       3|       164.52|  8.226| 172.746|02-20-2019|13:27| Credit card|       8.226|   5.9|
|351-62-0822|     B|       Member|Female| Fashion accessories|     14.48|       4|        57.92|  2.896|  60.816|02-06-2019|18:07|     Ewallet|       2.896|   4.5|
|529-56-3974|     B|       Member|  Male|Electronic access...|     25.51|       4|       102.04|  5.102| 107.142|03-09-2019|17:03|        Cash|       5.102|   6.8|
|365-64-0515|     A|       Normal|Female|Electronic access...|     46.95|       5|       234.75|11.7375|246.4875|02-12-2019|10:25|     Ewallet|     11.7375|   7.1|
|252-56-2699|     A|       Normal|  Male|  Food and beverages|     43.19|      10|        431.9| 21.595| 453.495|02-07-2019|16:48|     Ewallet|      21.595|   8.2|
|829-34-3910|     A|       Normal|Female|   Health and beauty|     71.38|      10|        713.8|  35.69|  749.49|03-29-2019|19:21|        Cash|       35.69|   5.7|
|299-46-1805|     B|       Member|Female|   Sports and travel|     93.72|       6|       562.32| 28.116| 590.436|01-15-2019|16:19|        Cash|      28.116|   4.5|
|656-95-9349|     A|       Member|Female|   Health and beauty|     68.93|       7|       482.51|24.1255|506.6355|03-11-2019|11:03| Credit card|     24.1255|   4.6|
|765-26-6951|     A|       Normal|  Male|   Sports and travel|     72.61|       6|       435.66| 21.783| 457.443|01-01-2019|10:39| Credit card|      21.783|   6.9|
|329-62-1586|     A|       Normal|  Male|  Food and beverages|     54.67|       3|       164.01| 8.2005|172.2105|01-21-2019|18:00| Credit card|      8.2005|   8.6|
|319-50-3348|     B|       Normal|Female|  Home and lifestyle|      40.3|       2|         80.6|   4.03|   84.63|03-11-2019|15:30|     Ewallet|        4.03|   4.4|
+-----------+------+-------------+------+--------------------+----------+--------+-------------+-------+--------+----------+-----+------------+------------+------+
only showing top 20 rows
*/


//sorting it by data and time

val sortByDateTime=SalesRDD.sortBy(_._12).sortBy(_._11)
//sortByDateTime: org.apache.spark.rdd.RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = MapPartitionsRDD[15] at sortBy at <console>:28

sortByDateTime.toDF("invoice_id", "branch", "customer_type", "gender", "product", "unit_price", "quantity", "cost_of_goods","tax", "total", "date", "time", "payment_type", "gross_income", "rating").show()
/*
+-----------+------+-------------+------+--------------------+----------+--------+-------------+-------+--------+----------+-----+------------+------------+------+
| invoice_id|branch|customer_type|gender|             product|unit_price|quantity|cost_of_goods|    tax|   total|      date| time|payment_type|gross_income|rating|
+-----------+------+-------------+------+--------------------+----------+--------+-------------+-------+--------+----------+-----+------------+------------+------+
|765-26-6951|     A|       Normal|  Male|   Sports and travel|     72.61|       6|       435.66| 21.783| 457.443|01-01-2019|10:39| Credit card|      21.783|   6.9|
|746-04-1077|     B|       Member|Female|  Food and beverages|     84.63|      10|        846.3| 42.315| 888.615|01-01-2019|11:36| Credit card|      42.315|     9|
|271-77-8740|     C|       Member|Female|   Sports and travel|     29.22|       6|       175.32|  8.766| 184.086|01-01-2019|11:40|     Ewallet|       8.766|     5|
|133-14-7229|     C|       Normal|  Male|   Health and beauty|     62.87|       2|       125.74|  6.287| 132.027|01-01-2019|11:43|        Cash|       6.287|     5|
|651-88-7328|     A|       Normal|Female| Fashion accessories|     65.74|       9|       591.66| 29.583| 621.243|01-01-2019|13:55|        Cash|      29.583|   7.7|
|416-17-9926|     A|       Member|Female|Electronic access...|     74.22|      10|        742.2|  37.11|  779.31|01-01-2019|14:42| Credit card|       37.11|   4.3|
|530-90-9855|     A|       Member|  Male|  Home and lifestyle|     47.59|       8|       380.72| 19.036| 399.756|01-01-2019|14:47|        Cash|      19.036|   5.7|
|556-97-7101|     C|       Normal|Female|Electronic access...|     63.22|       2|       126.44|  6.322| 132.762|01-01-2019|15:51|        Cash|       6.322|   8.5|
|891-01-7034|     B|       Normal|Female|Electronic access...|     74.71|       6|       448.26| 22.413| 470.673|01-01-2019|19:07|        Cash|      22.413|   6.7|
|770-42-8960|     B|       Normal|  Male|  Food and beverages|     21.12|       8|       168.96|  8.448| 177.408|01-01-2019|19:31|        Cash|       8.448|   6.3|
|493-65-6248|     C|       Member|Female|   Sports and travel|     36.98|      10|        369.8|  18.49|  388.29|01-01-2019|19:48| Credit card|       18.49|     7|
|182-52-7000|     A|       Member|Female|   Sports and travel|     27.04|       4|       108.16|  5.408| 113.568|01-01-2019|20:26|     Ewallet|       5.408|   6.9|
|244-08-0162|     B|       Normal|Female|   Health and beauty|     34.21|      10|        342.1| 17.105| 359.205|01-02-2019|13:00|        Cash|      17.105|   5.1|
|198-84-7132|     B|       Member|  Male| Fashion accessories|     40.61|       9|       365.49|18.2745|383.7645|01-02-2019|13:40|        Cash|     18.2745|     7|
|712-39-0363|     A|       Member|  Male|  Food and beverages|     41.66|       6|       249.96| 12.498| 262.458|01-02-2019|15:24|     Ewallet|      12.498|   5.6|
|345-68-9016|     C|       Member|Female|   Sports and travel|     31.67|       8|       253.36| 12.668| 266.028|01-02-2019|16:19| Credit card|      12.668|   5.6|
|504-35-8843|     A|       Normal|  Male|   Sports and travel|     42.47|       1|        42.47| 2.1235| 44.5935|01-02-2019|16:57|        Cash|      2.1235|   5.7|
|446-47-6729|     C|       Normal|  Male| Fashion accessories|     99.82|       2|       199.64|  9.982| 209.622|01-02-2019|18:09| Credit card|       9.982|   6.7|
|744-09-5786|     B|       Normal|  Male|Electronic access...|     22.01|       6|       132.06|  6.603| 138.663|01-02-2019|18:50|        Cash|       6.603|   7.6|
|670-71-7306|     B|       Normal|  Male|   Sports and travel|     44.63|       6|       267.78| 13.389| 281.169|01-02-2019|20:08| Credit card|      13.389|   5.1||
+-----------+------+-------------+------+--------------------+----------+--------+-------------+-------+--------+----------+-----+------------+------------+------+
only showing top 20 rows
*/


val SalesModifiedRDD = data.map { l =>
  val s = l.split(',')
  val (invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods, tax, total, date, time, payment_type, gross_income, rating) = (s(0), s(1), s(2), s(3), s(4), s(5), s(6).toInt, s(7), s(8), s(9).toFloat, s(10), s(11).replaceAll(":", "").toInt, s(12), s(13), s(14).toFloat)
  (invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods, tax, total, date, time, payment_type, gross_income, rating)
}
//SalesModifiedRDD: org.apache.spark.rdd.RDD[(String, String, String, String, String, String, String, String, String, String, String, Int, String, String, Float)] = MapPartitionsRDD[20] at map at <console>:26


//number of customers during a time period

val numberOfCustomers=SalesModifiedRDD.filter{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating)=>(time>=1000 && time<=1500)}
//numberOfCustomers: org.apache.spark.rdd.RDD[(String, String, String, String, String, String, String, String, String, String, String, Int, String, String, String)] = MapPartitionsRDD[21] at filter at <console>:28

numberOfCustomers.count()
//res2: Long = 467


//number of customers on a particular date

val customersOnParticularDate=SalesModifiedRDD.map{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating) =>(date,1)}
//customersOnParticularDate: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[3] at map at <console>:28

val customersSortedByDate=customersOnParticularDate.reduceByKey(_+_).sortBy(_._1)
//customersSortedByDate: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[9] at sortBy at <console>:30

customersSortedByDate.toDF("Date(m/d/y)","customers").show(20)
/*
+-----------+---------+
|Date(m/d/y)|customers|
+-----------+---------+
| 01-01-2019|       12|
| 01-02-2019|        8|
| 01-03-2019|        8|
| 01-04-2019|        6|
| 01-05-2019|       12|
| 01-06-2019|        9|
| 01-07-2019|        9|
| 01-08-2019|       18|
| 01-09-2019|        8|
| 01-10-2019|        9|
| 01-11-2019|        8|
| 01-12-2019|       11|
| 01-13-2019|       10|
| 01-14-2019|       13|
| 01-15-2019|       13|
| 01-16-2019|       10|
| 01-17-2019|       11|
| 01-18-2019|        9|
| 01-19-2019|       16|
| 01-20-2019|       10|
+-----------+---------+
only showing top 20 rows
*/

val AvgCustomersInTime=numberOfCustomers.count()/customersSortedByDate.count()
//AvgCustomersInTime: Long = 5


//number of customers on january 1

val customersOnJan1=customersSortedByDate.lookup("01-01-2019")(0)
//customersOnJan1: Int = 12



//Highest number of customers on a day
val HighestCustomers=customersOnParticularDate.reduceByKey(_+_).sortBy(-_._2)
//HighestCustomers: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[120] at sortBy at <console>:30

HighestCustomers.toDF("Date(m/d/y)","customers").show(10)
/*
+-----------+---------+
|Date(m/d/y)|customers|
+-----------+---------+
| 02-07-2019|       20|
| 02-15-2019|       19|
| 03-14-2019|       18|
| 03-02-2019|       18|
| 01-08-2019|       18|
| 03-05-2019|       17|
| 01-23-2019|       17|
| 01-25-2019|       17|
| 01-26-2019|       17|
| 03-09-2019|       16|
+-----------+---------+
only showing top 10 rows
*/


//Least number of customers on a day

val LeastCustomers=customersOnParticularDate.reduceByKey(_+_).sortBy(_._2)
//LeastCustomers: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[129] at sortBy at <console>:30

LeastCustomers.toDF("Date(m/d/y)","customers").show(10)
/*
+-----------+---------+
|Date(m/d/y)|customers|
+-----------+---------+
| 02-28-2019|        6|
| 03-21-2019|        6|
| 02-01-2019|        6|
| 01-04-2019|        6|
| 02-21-2019|        6|
| 03-17-2019|        6|
| 03-18-2019|        7|
| 01-22-2019|        7|
| 02-18-2019|        7|
| 01-09-2019|        8|
+-----------+---------+
only showing top 10 rows
*/


//number of customers by payment type

val paymentType=SalesModifiedRDD.map{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating) =>(payment_type,1)}
//paymentType: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[15] at map at <console>:28

val paymentTypeSorted=paymentType.reduceByKey(_+_).sortBy(_._1)
//paymentTypeSorted: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[21] at sortBy at <console>:30

paymentTypeSorted.toDF("payment type","customers").show()
/*
+------------+---------+
|payment type|customers|
+------------+---------+
|        Cash|      344|
| Credit card|      311|
|     Ewallet|      345|
+------------+---------+
*/




val paymentTypeOfBranch = SalesModifiedRDD.filter(_._2 == "A").map{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating) =>(payment_type,1)}
//paymentTypeOfBranch: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[26] at map at <console>:28

val paymentTypeofBranchSorted=paymentTypeOfBranch.reduceByKey(_+_).sortBy(_._2)
//paymentTypeofBranchSorted: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[32] at sortBy at <console>:30

paymentTypeofBranchSorted.toDF("Mode of Payment", "No of customers").show()
/*
+---------------+---------------+
|Mode of Payment|No of customers|
+---------------+---------------+
|    Credit card|            104|
|           Cash|            110|
|        Ewallet|            126|
+---------------+---------------+
*/


 val total=paymentTypeofBranchSorted.values.reduce(_+_)
//total: Int = 340

//probability of number of customers using cash
val cashProbability=(paymentTypeofBranchSorted.filter(_._1 == "Cash").values.collect()(0)/total.toFloat)*100
//cashProbability: Float = 32.352943

//probability of number of customers using credit
val creditCardProbability=(paymentTypeofBranchSorted.filter(_._1 == "Credit card").values.collect()(0)/total.toFloat)*100
//creditCardProbability: Float = 30.588236

//probability of number of customers using Ewallet
val EwalletProbability=(paymentTypeofBranchSorted.filter(_._1 == "Ewallet").values.collect()(0)/total.toFloat)*100
//EwalletProbability: Float = 37.058823



// sales by product category on a single day

val salesOnSingleDay=SalesModifiedRDD.map{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating) =>(date,(product,1))}
//salesOnSingleDay: org.apache.spark.rdd.RDD[(Int, (String, Int))] = MapPartitionsRDD[56] at map at <console>:28

val salesOnJan1 = salesOnSingleDay.lookup("01-01-2019")
//salesOnJan1: Seq[(String, Int)] = WrappedArray((Sports and travel,1), (Home and lifestyle,1), (Electronic accessories,1), (Sports and travel,1), (Electronic accessories,1), (Health and beauty,1), (Fashion accessories,1), (Sports and travel,1), (Electronic accessories,1), (Sports and travel,1), (Food and beverages,1), (Food and beverages,1))


val salesOnJan1Reduced = sc.parallelize(salesOnJan1).reduceByKey(_+_).sortBy(_._2)
//salesOnJan1Reduced: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[75] at sortBy at <console>:32

salesOnJan1Reduced.toDF("Category","number of trades on 01-01-2019").show()
/*
+--------------------+------------------------------+
|            Category|number of trades on 01-01-2019|
+--------------------+------------------------------+
| Fashion accessories|                             1|
|   Health and beauty|                             1|
|  Home and lifestyle|                             1|
|  Food and beverages|                             2|
|Electronic access...|                             3|
|   Sports and travel|                             4|
+--------------------+------------------------------+
*/



//number of customers by gender purchasing different category products

 val productCategoriesByGender=SalesModifiedRDD.map{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating) =>(gender,(product,1))}
//productCategoriesByGender: org.apache.spark.rdd.RDD[(String, (String, Int))] = MapPartitionsRDD[169] at map at <console>:28

val categoriesByMale=productCategoriesByGender.lookup("Male")
//categoriesByMale: Seq[(String, Int)] = WrappedArray((Home and lifestyle,1), (Health and beauty,1), (Sports and travel,1), (Electronic accessories,1), (Electronic accessories,1), (Food and beverages,1), (Sports and travel,1), (Food and beverages,1), (Electronic accessories,1), (Health and beauty,1), (Home and lifestyle,1), (Electronic accessories,1), (Sports and travel,1), (Fashion accessories,1), (Health and beauty,1), (Fashion accessories,1), (Sports and travel,1), (Sports and travel,1), (Health and beauty,1), (Sports and travel,1), (Home and lifestyle,1), (Home and lifestyle,1), (Health and beauty,1), (Health and beauty,1), (Electronic accessories,1), (Food and beverages,1), (Fashion accessories,1), (Home and lifestyle,1), (Home and lifestyle,1), (Health and beauty,1), (Electronic acc...

 val categoriesByMaleSorted=sc.parallelize(categoriesByMale).reduceByKey(_+_).sortBy(_._2)
//categoriesByMaleSorted: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[178] at sortBy at <console>:32

categoriesByMaleSorted.toDF("Product Category","No of Male customers").show()
/*
+--------------------+--------------------+
|    Product Category|No of Male customers|
+--------------------+--------------------+
|   Sports and travel|                  78|
|  Home and lifestyle|                  81|
| Fashion accessories|                  82|
|  Food and beverages|                  84|
|Electronic access...|                  86|
|   Health and beauty|                  88|
+--------------------+--------------------+
*/

 val categoriesByFemale=productCategoriesByGender.lookup("Female")
//categoriesByFemale: Seq[(String, Int)] = WrappedArray((Health and beauty,1), (Electronic accessories,1), (Electronic accessories,1), (Home and lifestyle,1), (Health and beauty,1), (Food and beverages,1), (Fashion accessories,1), (Electronic accessories,1), (Health and beauty,1), (Sports and travel,1), (Health and beauty,1), (Home and lifestyle,1), (Home and lifestyle,1), (Fashion accessories,1), (Food and beverages,1), (Food and beverages,1), (Sports and travel,1), (Electronic accessories,1), (Health and beauty,1), (Home and lifestyle,1), (Sports and travel,1), (Food and beverages,1), (Electronic accessories,1), (Food and beverages,1), (Fashion accessories,1), (Food and beverages,1), (Fashion accessories,1), (Electronic accessories,1), (Home and lifestyle,1), (Sports and travel,1), (Spo...
 val categoriesByFemaleSorted=sc.parallelize(categoriesByFemale).reduceByKey(_+_).sortBy(_._2)
//categoriesByFemaleSorted: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[195] at sortBy at <console>:32

 categoriesByFemaleSorted.toDF("Product Category","No of Female customers").show()
/*
+--------------------+----------------------+
|    Product Category|No of Female customers|
+--------------------+----------------------+
|   Health and beauty|                    64|
|  Home and lifestyle|                    79|
|Electronic access...|                    84|
|   Sports and travel|                    88|
|  Food and beverages|                    90|
| Fashion accessories|                    96|
+--------------------+----------------------+
*/



//number of customers on different branches

val customersByBranch=SalesModifiedRDD.map{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating) =>(branch,1)}
//customersByBranch: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[43] at map at <console>:28

val customersByBranchSorted=customersByBranch.reduceByKey(_+_).sortBy(_._1)
//customersByBranchSorted: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[49] at sortBy at <console>:30


customersByBranchSorted.toDF("Branch", "No of customers").show()
/*
+------+---------------+
|Branch|No of customers|
+------+---------------+
|     A|            340|
|     B|            332|
|     C|            328|
+------+---------------+
*/


val branchRating=SalesModifiedRDD.map{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating)=>(branch,rating)}
//branchRating: org.apache.spark.rdd.RDD[(String, Float)] = MapPartitionsRDD[343] at map at <console>:38


branchRating.toDF("Branch","Rating").show()
/*
+------+------+
|Branch|Rating|
+------+------+
|     A|   9.1|
|     C|   9.6|
|     A|   7.4|
|     A|   8.4|
|     A|   5.3|
|     C|   4.1|
|     A|   5.8|
|     C|   8.0|
|     A|   7.2|
|     B|   5.9|
|     B|   4.5|
|     B|   6.8|
|     A|   7.1|
|     A|   8.2|
|     A|   5.7|
|     B|   4.5|
|     A|   4.6|
|     A|   6.9|
|     A|   8.6|
|     B|   4.4|
+------+------+
only showing top 20 rows
*/


//finding average rating for all the branches

val customersBranchA=customersByBranchSorted.lookup("A")(0)
//customersBranchA: Int = 340

val AvgRatingBranchA=branchRating.filter(_._1 == "A").values.reduce(_+_)/customersBranchA
//AvgRatingBranchA: Float = 7.027058

val customersBranchB=customersByBranchSorted.lookup("B")(0)
//customersBranchB: Int = 332

val AvgRatingBranchB=branchRating.filter(_._1 == "B").values.reduce(_+_)/customersBranchB
//AvgRatingBranchB: Float = 6.818072

val customersBranchC=customersByBranchSorted.lookup("C")(0)
//customersBranchC: Int = 328

val AvgRatingBranchC=branchRating.filter(_._1 == "C").values.reduce(_+_)/customersBranchC
//AvgRatingBranchC: Float = 7.0728655


//total average rating of the supermaket store

val total=customersByBranchSorted.values.reduce(_+_)
//total: Int = 1000

val AvgRating=branchRating.values.reduce(_+_)/total
//AvgRating: Float = 6.9727006



// products purchased based on quauntity

val productQuantity=SalesModifiedRDD.map{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating)=>(product, quantity)}
//productQuantity: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[39] at map at <console>:28

val productQuantitySorted=productQuantity.reduceByKey(_+_).sortBy(_._1)
//productQuantitySorted: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[45] at sortBy at <console>:30

productQuantitySorted.toDF("Product", "Quantity").show(false)
/*
+----------------------+--------+
|Product               |Quantity|
+----------------------+--------+
|Electronic accessories|971     |
|Fashion accessories   |902     |
|Food and beverages    |952     |
|Health and beauty     |854     |
|Home and lifestyle    |911     |
|Sports and travel     |920     |
+----------------------+--------+
*/


// quantity of products purchased by member type of customers

val productQuantityByCustomerMember=SalesModifiedRDD.filter(_._3 == "Member").map{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating)=>(product,quantity)}
//productQuantityByCustomerMember: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[54] at map at <console>:28


val productQuantitySortedMember=productQuantityByCustomerMember.reduceByKey(_+_).sortBy(_._1)
//productQuantitySortedMember: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[69] at sortBy at <console>:30

productQuantitySortedMember.toDF("Product", "Quantity").show(false)
/*
+----------------------+--------+
|Product               |Quantity|
+----------------------+--------+
|Electronic accessories|429     |
|Fashion accessories   |439     |
|Food and beverages    |506     |
|Health and beauty     |428     |
|Home and lifestyle    |490     |
|Sports and travel     |493     |
+----------------------+--------+
*/


//quantity of product purchased by normal type of customers

val productQuantityByCustomerNormal=SalesModifiedRDD.filter(_._3 == "Normal").map{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating)=>(product,quantity)}
//productQuantityByCustomerNormal: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[245] at map at <console>:28

val productQuantitySortedNormal=productQuantityByCustomerNormal.reduceByKey(_+_).sortBy(_._1)
//productQuantitySortedNormal: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[251] at sortBy at <console>:30

productQuantitySortedNormal.toDF("Product", "Quantity").show(false)
/*
+----------------------+--------+
|Product               |Quantity|
+----------------------+--------+
|Electronic accessories|542     |
|Fashion accessories   |463     |
|Food and beverages    |446     |
|Health and beauty     |426     |
|Home and lifestyle    |421     |
|Sports and travel     |427     |
+----------------------+--------+
*/


//quantity of products purchased by customers(Member/Normal)

 val totalProductByCustomerType=(productQuantitySortedMember join productQuantitySortedNormal)
//totalProductByCustomerType: org.apache.spark.rdd.RDD[(String, (Int, Int))] = MapPartitionsRDD[268] at join at <console>:36

totalProductByCustomerType.toDF("Product","Member/Normal").show()
/*
+--------------------+-------------+
|             Product|Member/Normal|
+--------------------+-------------+
| Fashion accessories|    [439,463]|
|   Sports and travel|    [493,427]|
|Electronic access...|    [429,542]|
|  Food and beverages|    [506,446]|
|   Health and beauty|    [428,426]|
|  Home and lifestyle|    [490,421]|
+--------------------+-------------+
*/


//plotting of the graph

val file = new java.io.PrintStream("C:\\Users\\MITHIL\\Desktop\\output_files\\Plot1.csv")
//file: java.io.PrintStream = java.io.PrintStream@31c584c3

totalProductByCustomerType.collect.foreach(file.println(_))

file.close                                                                        




//total product rating

val productRating=SalesModifiedRDD.map{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating)=>(product,rating)}.reduceByKey(_+_)
//productRating: org.apache.spark.rdd.RDD[(String, Float)] = ShuffledRDD[22] at reduceByKey at <console>:28

 productRating.toDF("product","Total Rating").show(false)
/*
+----------------------+------------+
|product               |Total Rating|
+----------------------+------------+
|Fashion accessories   |1251.2      |
|Sports and travel     |1148.1001   |
|Electronic accessories|1177.2001   |
|Food and beverages    |1237.7002   |
|Health and beauty     |1064.5      |
|Home and lifestyle    |1094.0001   |
+----------------------+------------+
*/


//total number of customers by categories
 val customersByCategories=SalesModifiedRDD.map{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating) =>(product,1)}
//customersByCategories: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[133] at map at <console>:28

val customersByCategoriesSorted=customersByCategories.reduceByKey(_+_).sortBy(_._2)
//customersByCategoriesSorted: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[148] at sortBy at <console>:30

customersByCategoriesSorted.toDF("product category","customers").show()
/*
+--------------------+---------+
|    product category|customers|
+--------------------+---------+
|   Health and beauty|      152|
|  Home and lifestyle|      160|
|   Sports and travel|      166|
|Electronic access...|      170|
|  Food and beverages|      174|
| Fashion accessories|      178|
+--------------------+
*/


val totalRating=(productRating join customersByCategoriesSorted)
//totalRating: org.apache.spark.rdd.RDD[(String, (Float, Int))] = MapPartitionsRDD[38] at join at <console>:34


totalRating.toDF("product","Total Rating, Number of customers").show()
/*
+--------------------+---------------------------------+
|             product|Total Rating, Number of customers|
+--------------------+---------------------------------+
| Fashion accessories|                     [1251.2,178]|
|   Sports and travel|                  [1148.1001,166]|
|Electronic access...|                  [1177.2001,170]|
|  Food and beverages|                  [1237.7002,174]|
|   Health and beauty|                     [1064.5,152]|
|  Home and lifestyle|                  [1094.0001,160]|
+--------------------+---------------------------------+
*/


//Average Rating for each product category

 val totalAvgRating=totalRating.map{case(_1,(a,b))=>(_1,(a/b))}
//totalAvgRating: org.apache.spark.rdd.RDD[(String, Float)] = MapPartitionsRDD[39] at map at <console>:36

totalAvgRating.toDF("Product","Average Rating").show()
/*
+--------------------+--------------+
|             Product|Average Rating|
+--------------------+--------------+
| Fashion accessories|     7.0292134|
|   Sports and travel|     6.9162655|
|Electronic access...|     6.9247065|
|  Food and beverages|     7.1132197|
|   Health and beauty|     7.0032897|
|  Home and lifestyle|     6.8375006|
+--------------------+--------------+
*/


//plotting the graph

val file = new java.io.PrintStream("C:\\Users\\MITHIL\\Desktop\\output_files\\Plot2.csv")
//file: java.io.PrintStream = java.io.PrintStream@31c584c3

 totalAvgRating.collect.foreach(file.println(_))

file.close


val SalesOnADay=SalesModifiedRDD.map{case(invoice_id, branch, customer_type, gender, product, unit_price, quantity, cost_of_goods,tax, total, date, time, payment_type, gross_income, rating)=>(date, total)}
//SalesOnADay: org.apache.spark.rdd.RDD[(String, Float)] = MapPartitionsRDD[3] at map at <console>:28

val salesOnADaySorted=SalesOnADay.reduceByKey(_+_).sortBy(_._1)
//salesOnADaySorted: org.apache.spark.rdd.RDD[(String, Float)] = MapPartitionsRDD[9] at sortBy at <console>:30

salesOnADaySorted.toDF("Date","Sales").show()
/*
+----------+---------+
|      Date|    Sales|
+----------+---------+
|01-01-2019|4745.1807|
|01-02-2019|1945.5029|
|01-03-2019|2078.1284|
|01-04-2019|1623.6886|
|01-05-2019|3536.6833|
|01-06-2019| 3614.205|
|01-07-2019| 2834.244|
|01-08-2019|5293.7324|
|01-09-2019|3021.3433|
|01-10-2019| 3560.949|
|01-11-2019|2114.9624|
|01-12-2019|5184.7637|
|01-13-2019| 2451.204|
|01-14-2019|3966.6167|
|01-15-2019|  5944.26|
|01-16-2019| 4289.082|
|01-17-2019|3142.7551|
|01-18-2019|2780.4734|
|01-19-2019|4914.7246|
|01-20-2019|3655.4487|
+----------+---------+
only showing top 20 rows
*/


val file = new java.io.PrintStream("C:\\Users\\MITHIL\\Desktop\\output_files\\Plot3.csv")
//file: java.io.PrintStream = java.io.PrintStream@31c584c3

 salesOnADaySorted.collect.foreach(file.println(_))

file.close

