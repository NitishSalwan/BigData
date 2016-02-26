import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class question1_new 
{
public static class Map1 extends Mapper<LongWritable, Text, Text,Text>
  {
	HashMap<String, String> usersMap = new HashMap<String,String >();
	
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		Path[] localPaths = context.getLocalCacheFiles();
		for(Path myfile:localPaths)
		{
			String line=null;
			String nameofFile=myfile.getName();
			File file =new File(nameofFile+"");
			FileReader fr= new FileReader(file);
			BufferedReader br= new BufferedReader(fr);
			line=br.readLine();
			while(line!=null)
			{
				String[] data=line.split("\t");
				if(data.length==1)
					usersMap.put(data[0],"");
				else
				usersMap.put(data[0],data[1]);
				line=br.readLine();
			}
		}
	}
    
	
    // type of output key
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
      {
    	String temporary1;
    	String temporary2;
    	StringTokenizer str_token1;
    	StringTokenizer str_token2;
    	String temporary;
    	String[] mydata = value.toString().split("\t");
    	String[] array;
    	
    	int counter=0;
    	if(mydata.length==2)
    	{
    		
    		str_token1=new StringTokenizer(mydata[1],",");
    		array=mydata[1].split(",");
    		
    		while(str_token1.hasMoreElements())
    		{
    			temporary=str_token1.nextElement().toString();
    			if(usersMap.containsKey(temporary))
    			{
    				temporary1=usersMap.get(temporary).toString();
    				str_token2=new StringTokenizer(temporary1,",");
    				while(str_token2.hasMoreElements())
    	    		{
    					temporary2=str_token2.nextElement().toString();
    					if(!temporary2.equals(mydata[0].toString()))
    					{
    						for(String text1 : array)
    						{	
    							if(text1.equals(temporary2))
    							{
    								counter=counter+1;
    								
    							}
    						}
    						if(counter==0)
    						context.write(new Text(mydata[0]),new Text(temporary2));
    					}
    	    		}
    					    					
    					
    					
    			}
    				
    				
    		}
    			
    	}
    		
    	
    	/*
    		StringTokenizer str_token1=new StringTokenizer(mydata[1],",");
    		while(str_token1.hasMoreElements())
    		{
    			context.write(new Text(mydata[0]),new Text(str_token1.toString() + "-" + "DIRECT"));
    		}
    	    
    		for(int i=0;i<array.length;i++)
    		{
    			for(int j=0;j<array.length;j++)
    			{
    				if(!(array[i].equals(array[j])))
    				{
    					context.write(new Text(array[i]),new Text(array[j] + "-" + "MUTUAL"));
    					context.write(new Text(array[j]),new Text(array[i] + "-" + "MUTUAL"));    					
    				}
    			}
    		}
    	
    	}
    	if(mydata.length==1)
    	{
    		context.write(new Text(mydata[0]),new Text(mydata[0] + "-" + "EMPTY"));
    	}
    */	
    	
    	
      }
    
  }
  
/*class neighbour
{
	
	private int id;
	private String nature;
	
	neighbour(int id,String nature)
	{    		
		this.id=id;
		this.nature=nature;
	}
	
	public int get_id()
	{
		return id;
	}
	
	public String get_nature()
	{
		return nature;
	}
}
*/
  
public static class Reduce1 extends Reducer<Text,Text,Text,Text> 
{
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
	
		OrderedArrayMaxPQ<pair> pq;
		HashMap<String,Integer> hmain=new HashMap<String,Integer>();
		
		for(Text text : values)
		{
						
			if(hmain.containsKey(text))
			{
				hmain.put(text.toString(),hmain.get(text.toString())+1);
				
			}
			else
				hmain.put(text.toString(),1);
			
		}
		
		
		
		
		 /*pq = new OrderedArrayMaxPQ<pair>(hmain.size());
		
		for(Map.Entry<String,Integer> entry : hmain.entrySet()){
			 pq.insert(new pair(entry.getKey(),entry.getValue()));
		 }
		
		StringBuilder temp_str = new StringBuilder();
		
		int elements=10;
		if(pq.size()<10)
			elements=pq.size();
		for(int i=0;i<elements;i++)
		{
			temp_str.append(pq.delMax().use); 
			temp_str.append(","); 
		}
		temp_str.deleteCharAt(temp_str.length()-1);*/
		StringBuilder temp_str = new StringBuilder();
		HashMap<String,Integer> map = sortByValues(hmain);
		//String str1=sortTop(hmain);
		int count = 0;
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			temp_str.append(entry.getKey());
			temp_str.append(",");
			count++;
			if(count==10)
				break;
		}
		temp_str.deleteCharAt(temp_str.length()-1);
		context.write(key,new Text(temp_str.toString()));
		
	}
	
	 private static HashMap sortByValues(HashMap map) { 
	       List list = new LinkedList(map.entrySet());
	       // Defined Custom Comparator here
	       Collections.sort(list, new Comparator() {
	            public int compare(Object o1, Object o2) {
	            	int compare = ((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue());
	            	if(compare == 0 ){
	            		return ((Comparable) ((Map.Entry) (o1)).getKey())
	      	                  .compareTo(((Map.Entry) (o2)).getKey());
	            	}
	            	else
	            		return compare;
	            }
	       });

	       // Here I am copying the sorted list in HashMap
	       // using LinkedHashMap to preserve the insertion order
	       HashMap sortedHashMap = new LinkedHashMap();
	       for (Iterator it = list.iterator(); it.hasNext();) {
	              Map.Entry entry = (Map.Entry) it.next();
	              sortedHashMap.put(entry.getKey(), entry.getValue());
	       } 
	       return sortedHashMap;
	  }
	
	/*
	public Map<String,Integer> getMap() 
    {
      if(null == hmain)
         hmain = new HashMap<String,Integer>(0);
      	return hmain;
    	 }
    */
	
	/*public static <K, V extends Comparable<? super V>> Map<K, V> 
    sortByValue( Map<K, V> map )
{
    List<Map.Entry<K, V>> list = new LinkedList<>( map.entrySet() );
    Collections.sort( list, new Comparator<Map.Entry<K, V>>()
    {
        @Override
        public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
        {
            return (o1.getValue()).compareTo( o2.getValue() );
        }
    } );

    Map<K, V> result = new LinkedHashMap<>();
    for (Map.Entry<K, V> entry : list)
    {
        result.put( entry.getKey(), entry.getValue() );
    }
    return result;
}
*/	
	
	public String sortTop (HashMap hmainn){
		Object[] string_user =  hmainn.keySet().toArray();
		Object Count[] = hmainn.values().toArray();
		
		int i, j;
		Object temp, tempcount;
		StringBuffer returnlist = new StringBuffer();
		for(i=0; i<Count.length; i++) {
			for(j=0; j<Count.length-1 ; j++){
				if(Integer.parseInt(Count[j].toString()) < Integer.parseInt(Count[j+1].toString()) ) {
					
					tempcount = Count[j+1];
					Count [j+1] = Count[j];
					Count [j] = tempcount;
					
					temp = string_user[j+1];
					string_user [j+1] = string_user[j];
					string_user [j] = temp;
 				}
			}
		}
		returnlist.append("\t");
		for(i=0; i<10 && i<string_user.length; i++) {
			returnlist.append(string_user[i].toString()).append(",");
		}
		returnlist.deleteCharAt(returnlist.length()-1); 
		
		
		return returnlist.toString();
	}
	
	class pair implements Comparable<pair>
	{

		String use;
		int counter;
		
		public pair(String user,int count)
		{
			use=user;
			counter=count;
			
		}
		
		
		
		private int getCount(String user)
		{
			return counter;
			
		}


	/*
		@Override
		public int compareTo(Object arg0) {
			pair a=(pair)arg0;
			return this.counter - a.counter ;
		}
	*/


		@Override
		public int compareTo(pair o) {
			// TODO Auto-generated method stub
			if(this.counter==o.counter)
			{
				return (Integer.parseInt(o.use) - Integer.parseInt(this.use));
			}
			else
			return (this.counter - o.counter) ;
			
		}
		
		 
	}

	
	
	
}

// Driver program
public static void main(String[] args) throws Exception 
    {
	Configuration conf = new  Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	// get all args
	if(otherArgs.length != 3) 
	{
		System.err.println("Usage: question1_new <in> <out>");
		System.exit(2);
	}

	
Job job = new Job(conf, "question1_new"); 

job.setJarByClass(question1_new.class);
job.setMapperClass(Map1.class);
job.setReducerClass(Reduce1.class);

job.addCacheFile(new URI(otherArgs[2]));

// uncomment the following line to add the Combiner 
//job.setCombinerClass(Reduce.class);
// set output key type 
job.setOutputKeyClass(Text.class);
// set output value type 
job.setOutputValueClass(Text.class);
//set the HDFS path of the input data
FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
// set the HDFS path for the output
FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//Wait till job completion
System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}