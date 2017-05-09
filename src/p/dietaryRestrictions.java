package p;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;

public class dietaryRestrictions {
	
    public static class Map1
    extends Mapper<Object, Text, Text, Text>{

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm");

        String word = "";
        String feeling = "";
        Date feelingDate = new Date();
        String food = "";
        Date foodDate = new Date();
        private Text feel = new Text();
        private Text foodItem = new Text();
        Map<Text,Date> feelingList = new HashMap<Text,Date>();

        public void map(Object key, Text value, Context context
        		) throws IOException, InterruptedException {
        	
        	String[] arr = value.toString().split(" ");    

        	int i = 0;
        	for(; i < arr.length; i++) {

        		word = arr[i];
        		i++;
//        		System.out.println("Word: " + word);
        		if(word.equals("feel"))
        		{
        			feeling = arr[i];
        			i++;
        			i++; //increment twice b/c douple space in input is being counted as a token
//        			System.out.println("feeling: " + feeling);
        			try {
        				String dateWithTime = arr[i]; //yyyy/MM/dd
        				i++; 
//        				System.out.println("feelDate: " + dateWithTime);
        				dateWithTime += " ";
        				dateWithTime += arr[i]; //HH:mm
        				i++;
//        				System.out.println("feelDateWithTime: " + dateWithTime);
        				feelingDate = dateFormat.parse(dateWithTime);
        			} catch (ParseException e) {
        				e.printStackTrace();
        			}
        			
        			feelingList.put(new Text(feeling), feelingDate);
        		}
        		else if(word.equals("food"))
        		{
        			food = arr[i];
        			i++;
        			i++;// increment twice b/c douple space in input being counted as a token
//        			System.out.println("food: " + food);
        			try {
        				String dateWithTime  = arr[i];
        				i++;
//        				System.out.println("foodDate: " + dateWithTime);
        				dateWithTime += " ";
        				dateWithTime += arr[i];
        				i++;
//        				System.out.println("foodDateWithTime: " + dateWithTime);
        				foodDate = dateFormat.parse(dateWithTime);
        			} catch (ParseException e) {
        				e.printStackTrace();
        			}
        			
        			Iterator entries = feelingList.entrySet().iterator();
        			while (entries.hasNext()) {
        				Entry thisEntry = (Entry) entries.next();
        				Text k = (Text) thisEntry.getKey();
        				Date v = (Date) thisEntry.getValue();
        				long diff = Math.abs(foodDate.getTime() - v.getTime());
        				long twelveHrsInMilliS = 3600000 *12;
        				if(diff <= twelveHrsInMilliS)
        				{
        					feel = k;
        					foodItem = new Text(food);
        					context.write(feel, foodItem);
        				}

        			}
        		}
        	} 
        }
    }


    public static class Reduce1
    extends Reducer<Text,Text, Text, Pair<Text, IntWritable>> {
                       
    	int itemCount;
    	HashMap<Pair<Text, Text> ,IntWritable> feelFoodToCount = new HashMap<Pair<Text, Text> ,IntWritable>();

        public void reduce(Text key, Iterable<Text> foodItems,
                           Context context
                           ) throws IOException, InterruptedException {
            for (Text foodItem : foodItems) {
				System.out.println("feel: " + key + " foodItem: " + foodItem); // testing
				Pair<Text, Text> feelFood = new Pair(key, foodItem);
				if(feelFoodToCount.get(feelFood) != null)
				{
					itemCount = feelFoodToCount.get(feelFood).get();
					//will replace old value
					feelFoodToCount.remove(feelFood);
					System.out.println("itemCount: " + itemCount); //testing
				}
				else
				{
					itemCount = 0;
					System.out.println("itemCount: " + itemCount); //testing
				}
				itemCount++;
				System.out.println("newItemCount: " + itemCount); // testing
				feelFoodToCount.put(feelFood, new IntWritable(itemCount));
            }
            

            Iterator entries = feelFoodToCount.entrySet().iterator();
            while (entries.hasNext()) {
            	Entry thisEntry = (Entry) entries.next(); 
            	Pair<Text, Text> fF =  (Pair<Text, Text>) thisEntry.getKey(); //will cast work here???
            	Text k = (Text) fF.getKey();
            	Text v = (Text) fF.getValue();
            	int itemCount =  Integer.parseInt(thisEntry.getValue().toString());
            	Pair<Text, IntWritable> foodCount= new Pair(v, new IntWritable(itemCount));
            	context.write(k, foodCount);
            }
            
        }

    }
    
//    public static class Map2
//    extends Mapper<Text, Pair<Text, IntWritable>, Text, Pair<Text, IntWritable>>{
//
//    	HashMap<Text, Pair<Text, IntWritable>> fC = new HashMap<Text, Pair<Text, IntWritable>>();
//
//    	public void map(Text key, Iterable<Pair<Text, IntWritable>> foodCountList, Context context
//    			) throws IOException, InterruptedException {
//    		int maxCount = 0;
//    		for (Pair<Text, IntWritable> foodCount : foodCountList) {
//
//    			if(foodCount.getValue().get() > maxCount)
//    			{
//    				if(fC.get(key) == null)
//    				{
//    					fC.put(key, foodCount);
//    				}
//    				else
//    				{
//    					fC.remove(key);
//    					fC.put(key, foodCount);
//    				}
//    			}
//    		}
//
//    		Iterator entries = fC.entrySet().iterator();
//    		while (entries.hasNext()) {
//    			Entry thisEntry = (Entry) entries.next(); 
//    			Text k = (Text) thisEntry.getKey();
//    			context.write(k, (Pair<Text, IntWritable>) thisEntry.getValue());
//    		}
//    	}
//    }
//    
//    
//    public static class Reduce2
//    extends Reducer<Text,Pair<Text, IntWritable>, Text, Text> {
//
//    	public void reduce(Text key, Iterable<Pair<Text, IntWritable>> foodCountList,
//    			Context context
//    			) throws IOException, InterruptedException {
//    		for (Pair<Text, IntWritable> foodCount : foodCountList) {
//    			
//    			Text result = new Text(", " + foodCount.getKey() + ", " + foodCount.getValue().toString());
//    			context.write(key, result);
//    		}
//    	}
//    }
    
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("6234", "hdfs://localhost:9000");
        
        Job job = Job.getInstance(conf, "job one");
        job.setJarByClass(dietaryRestrictions.class);
        job.setMapperClass(Map1.class);
        job.setCombinerClass(Reduce1.class);
        job.setReducerClass(Reduce1.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("temp"));
        job.waitForCompletion(true);

//        //JOB 2
//        Job job2 = Job.getInstance(conf, "job two");
//        job2.setJarByClass(dietaryRestrictions.class);
//        job2.setMapperClass(Map2.class);
//        job2.setCombinerClass(Reduce2.class);
//        job2.setReducerClass(Reduce2.class);
//        
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        
//        FileInputFormat.addInputPath(job2, new Path("temp"));
//        FileOutputFormat.setOutputPath(job2, new Path("output"));
//        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        
        System.out.println("Job Completed");
      }
    
}







