import java.util.Calendar

implicit val sortLongByString = new Ordering[Long] {
  override def compare(a: Long, b: Long) = b.compare(a)
}

def ageInYearMonthDays(dob : Long) : String = {
   val c = Calendar.getInstance()
    	c.setTimeInMillis(dob)
    	val mYear = c.get(Calendar.YEAR)-1970
    	val mMonth = c.get(Calendar.MONTH)
    	val mDay = c.get(Calendar.DAY_OF_MONTH)-1
    	return mYear + "yrs " + mMonth + "mths " +mDay+ "days" 
}

def convertDobToMS(dob : String) : Long = {
  val dateParts = dob.split("/")
  val c = Calendar.getInstance()
    c.set(Calendar.HOUR, 0)
    c.set(Calendar.MINUTE, 0)
    c.set(Calendar.SECOND, 0)
    c.set(Calendar.MILLISECOND, 0)
    val currentTime = c.getTimeInMillis()
    c.set(dateParts(2).toInt,dateParts(0).toInt-1, dateParts(1).toInt, 0, 0, 0)
    return currentTime - c.getTimeInMillis()
}

def makeInfo11(line : String) = { 
 val testInfo = line.split(",")
 (testInfo(0),convertDobToMS(testInfo(9)))
}

def show(x: Option[Long]) = x match {
      case Some(s) => s
      case None => 0
   }
 

def makeKey2(line: String) : List[(String,String)] = { 
     val lineParts = line.split("\\s+")
     if(lineParts.length==2)
     {
       val friendListStr = lineParts(1)
	     val friendList = friendListStr.split(",")
           
	  val pairs  = for(myString <- friendList) yield (myString, lineParts(0))	
       return  pairs.toList
     }
 	return List()     
    
}

 val userData = sc.textFile("/usr/local/spark/bin/userdata.txt")
 val friendData = sc.textFile("/usr/local/spark/bin/soc-LiveJournal1Adj.txt")

 val testMap11 = friendData.flatMap(line => {makeKey2(line)})

 val testMap88 = userData.map(line => {makeInfo11(line)})

 val testMap99 = testMap11.join(testMap88)

 val testMap22 = testMap99.map { case (t,(k,v)) => (k,v)}

type ageCollector = (Int, Long)
type PersonAges = (String, (Int, Long))

val createAgeCombiner = (age: Long) => (1, age)

val ageCombiner = (collector: ageCollector, age: Long) => {
         val (numberAges, totalAge) = collector
        (numberAges + 1, totalAge + age)
      }

val ageMerger = (collector1: ageCollector, collector2: ageCollector) => {
      val (numAge1, totalAge1) = collector1
      val (numAge2, totalAge2) = collector2
      (numAge1 + numAge2, totalAge1 + totalAge2)
    }
val ages = testMap22.combineByKey(createAgeCombiner, ageCombiner, ageMerger)
// val sortAgesRDD = ages.sortBy(_._2._2)
//val top20Data = sortAgesRDD.take(20)


val averagingFunction = (personAge: PersonAges) => {
       val (userId, (numberAges, totalAge)) = personAge
       (userId, totalAge / numberAges)
    }

val averageAges = ages.collectAsMap().map(averagingFunction)
val sortAverage = averageAges.toSeq.sortBy(_._2)
 val top20Data = sortAverage.take(20)
 val top20DataMap = top20Data.toMap
  val userInfo = userData.filter(line =>top20Data.exists(_._1 == line.split(",")(0)))
			.map(line => { 	
				  val splitIndex = line.indexOf(",")
                                  //println("test")
				  (line.substring(0,splitIndex),line.substring(splitIndex+1).split(",").toList)
				}).reduceByKey((a,b) => a ::: b) 
  val userInfoMap = userInfo.collectAsMap()
  userInfoMap.foreach(ps => println(ps._1 + " ||| " + ps._2(2)+", "+ ps._2(3)+", "+ ps._2(4)+", " +ps._2(5)+", "+ ps._2(6) + " ||| " + ageInYearMonthDays(show(top20DataMap.get(ps._1.toString()))) ))


