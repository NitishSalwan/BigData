def makeKey2(line: String) : List[(String,String)] = { 
     var l2 = scala.collection.mutable.ListBuffer[(String, String)]()
     val lineParts = line.split("\\s+")
       val tempString3 = (lineParts(0),lineParts(0)+"_C")
	l2 +=  tempString3
     if(lineParts.length==2)
     {
       l2.remove(0)
       val friendListStr = lineParts(1)
       val friendList = friendListStr.split(",")
           
	  for(myString <- friendList) 
		{
                        val tempString = (lineParts(0), myString+"_A")
			l2 += tempString	
                }
          for (i <- 0 until friendList.length)
          {
              for(j <-(i+1) until friendList.length)
                  {
                      val tempString1 = (friendList(i), friendList(j)+"_B")
			val tempString2 = (friendList(j), friendList(i)+"_B")
                     l2 += tempString1
                     l2 += tempString2
                  }
          }
	  return  l2.toList
     }
        
     return l2.toList   
}

def show(x: Option[Int]) = x match {
      case Some(s) => s
      case None => 0
   }

implicit val sortIntByString = new Ordering[Int] {
  override def compare(a: Int, b: Int) = b.compare(a)
}

 val friendData = sc.textFile("/usr/local/spark/bin/soc-LiveJournal1Adj.txt")
val testKeyMap = friendData.flatMap(line => {makeKey2(line)})
val testKeyMapFinal = testKeyMap.map(t => (t._1,t._2))
val testGroupKeyMap= testKeyMapFinal.groupByKey()

val finalResult = testGroupKeyMap.map(t => {  
                              val userId = t._1
                              val processString = t._2.toList
                              val setA = scala.collection.mutable.Set[String]()
                              val setB = scala.collection.mutable.Set[String]()
                              val setC = scala.collection.mutable.Set[String]()
                               var l2 = scala.collection.mutable.ListBuffer[(String)]()
                               val mutMap = scala.collection.mutable.Map[String, Int]() 
                              for(myString <- processString)
                              {
                                   val valPart = myString.split("_")
                                    l2 += valPart(0)
                                   if(valPart(1)=="A")
                                    {
                                        setA += valPart(0)
                                    }
                                   else if(valPart(1)=="B")
                                    {
                                      setB += valPart(0)
                                    }
                                    else 
                                    {
                                       (userId,"")
                                    } 
                                  
			      }
                          val matchSet = setB.diff(setA)
                          val eMap = l2.groupBy(l => l).map(t => (t._1, t._2.length))
                          
                          for(setStr <- matchSet)
                            mutMap += (setStr -> show(eMap.get(setStr)))
                           val sortMap = mutMap.toSeq.sortBy(_._2)
                            val builder = StringBuilder.newBuilder
 			    //builder.append( userId + "\t");
		            for(recommendId <- sortMap)
                                 builder.append(recommendId + ",")
 			   val ouputStr = builder.toString()
                           (userId,ouputStr)      
                      })


val displaySet = scala.collection.mutable.Set[String]("924", "8941", "8942", "9019", "9020", "9021", "9022", "9990", "9992", "9993")
finalResult.foreach(ps => {if(displaySet.contains(ps._1))println(ps._1 + "\t" + ps._2)})
