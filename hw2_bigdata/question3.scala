val user1 = readLine("prompt> ")
val user2 = readLine("prompt> ")
val lines = sc.textFile("file:///usr/local/spark/bin/soc-LiveJournal1Adj.txt")
val friends = lines.map(line=>line.split("\\t")).filter(l1 => (l1.size == 2)).filter(line=>(user2==line(0))).flatMap(line=>line(1).split(","))
val friends1 = lines.map(line=>line.split("\\t")).filter(l1 => (l1.size == 2)).filter(line=>(user1==line(0))).flatMap(line=>line(1).split(","))
val frd=friends1.intersection(friends).collect()
val result=user1+","+user2+"\t"+frd.mkString(",")
val userData = sc.textFile("file:///usr/local/spark/bin/userdata.txt")
val details1 = userData.map(line=>line.split(",")).filter(line=>frd.contains(line(0))).map(line=>(line(1)+":"+line(6)))
val answer=user1+","+user2+"\t"+"("+details1.collect.mkString(",")+")"

/**
val txtfile = sc.textFile("file:///usr/local/spark/bin/soc-LiveJournal1Adj.txt")
val wrt1 =txtfile.map(l => l.split("\t")).filter(l1 => (l1.size == 2)).filter(l1 => l1(0)==user1).flatMap(l => l(1).split(","))
val wrt2 =txtfile.map(l => l.split("\t")).filter(l1 => (l1.size == 2)).filter(l1 => l1(0)==user2).flatMap(l => l(1).split(","))
val wrt_final= wrt1.intersection(wrt2)
val userData = sc.textFile("file:///usr/local/spark/bin/userdata.txt")
val txtfile1 = sc.textFile("file:///usr/local/spark/bin/userdata.txt")
val wrt_final1=wrt_final.toArray()
val result =txtfile1.map(l => l.split(",")).filter(l1 => wrt_final1.contains(l1(0))).map(l => (l(0),(l(1),l(2),l(6))))
val answer=user1+","+user2+"\t"+"["+ result.collect.mkString(",")+"]"
*/



