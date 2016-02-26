import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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

public class question3 
{
public static class Map extends Mapper<LongWritable, Text, Text, Text>
  {
    private static String input1;
    private static String input2;
    HashMap<String, String> usersMap = new HashMap<String,String >();
    
	protected void setup(Context context)
			throws IOException, InterruptedException {
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
				String[] data=line.split(",");
				usersMap.put(data[0],data[1] + "!!" + data[2] + ":" + data[6]); 
				line=br.readLine();
			}
		}
	}
    
    
    // type of output key
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
      {
    	
    	StringTokenizer st_11;
    	StringBuilder str_build;
    	Configuration conf1=context.getConfiguration();
    	input1=conf1.get("input1").toString();
    	input2=conf1.get("input2").toString();
    	
    	int a1=Integer.parseInt(input1);
    	int b1=Integer.parseInt(input2);
    	   	
    	String[] mydata = value.toString().split("\t");
    	
    	if(mydata.length==2)
    	{
    		if((Integer.parseInt(mydata[0])==a1)||(Integer.parseInt(mydata[0])==b1))
    		{
    			 str_build = new StringBuilder();
    			 st_11= new StringTokenizer(mydata[1].toString(),",");
    			 
    			 while(st_11.hasMoreElements())
        		 {
    				String temporary=st_11.nextElement().toString();
    				str_build.append(temporary);
    				str_build.append("-");
    				str_build.append(usersMap.get(temporary));
    				str_build.append(",");
        		 }
    			 str_build.deleteCharAt(str_build.length()-1);
    			 context.write(new Text("1"),new Text(str_build.toString()));
    		}
    	}
    	if(mydata.length==1)
    	{
    		if((Integer.parseInt(mydata[0])==a1)||(Integer.parseInt(mydata[0])==b1))
    		{
    			context.write(new Text("1"),new Text("NULL"));
    		}
    		
    	}
    	
      }
  }

public static class Reduce extends Reducer<Text,Text,Text,Text> 
{
	
	ArrayList<String> lis1=new ArrayList<String>();
	ArrayList<String> lis2=new ArrayList<String>();
	
	private static String input1;
    private static String input2;
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		
		Configuration conf1=context.getConfiguration();
    	input1=conf1.get("input1").toString();
    	input2=conf1.get("input2").toString();
    	
    	
		
		for (Text text : values) 
		{
			
			
			StringTokenizer st= new StringTokenizer(text.toString(),",");
			if(lis1.size()==0)
			{
			 while(st.hasMoreElements())
    		 {
				lis1.add(st.nextElement().toString());
    		 }
			}
			else
			{
				while(st.hasMoreElements())
	    		 {
					lis2.add(st.nextElement().toString());
	    		 }
			}
			
	}
		
		lis1.retainAll(lis2);
		
		String output1=input1 + "\t" + input2;
		
		StringBuilder temp_str = new StringBuilder();
		temp_str.append("[");
		if(lis1.size()>0)
		{
		for (String s : lis1)
		{
			String[] temp_string= s.split("-");
			temp_str.append(temp_string[1].replace("!!", ","));
			temp_str.append(",");
		}
		temp_str.deleteCharAt(temp_str.length()-1);
		temp_str.append("]");
		context.write(new Text(output1),new Text(temp_str.toString()));
		}
	}
}

// Driver program
public static void main(String[] args) throws Exception 
    {
	Configuration conf = new  Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	// get all args
	if(otherArgs.length != 5) 
	{
		System.err.println("Usage: question3 <in> <out>");
		System.exit(2);
	}

	
	conf.set("input1", args[1]);
	conf.set("input2", args[2]);
	
Job job = new Job(conf, "question3"); 

job.setJarByClass(question3.class); 
job.setMapperClass(Map.class);
job.addCacheFile(new URI(otherArgs[4]));
job.setReducerClass(Reduce.class);
// uncomment the following line to add the Combiner 
//job.setCombinerClass(Reduce.class);
// set output key type 
job.setOutputKeyClass(Text.class);
// set output value type 
job.setOutputValueClass(Text.class);


//set the HDFS path of the input data
FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
// set the HDFS path for the output
FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
//Wait till job completion
System.exit(job.waitForCompletion(true) ? 0 : 1);
}}