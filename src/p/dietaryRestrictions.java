package p;

import java.io.IOException;
import java.io.PrintStream;
import java.util.StringTokenizer;
import java.util.TreeMap;

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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
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

    //count how many foods
    //reduce(feel, foodItemList)
    //	int counter = 0
    //	int maxCount = 0
    //	string maxFood = ""
    //	for each food in foodItemList
    //		food.counter++
    //	for each food in foodItemList
    //		if (food.count > maxCount)
    //			maxFood = food
    //			maxCount = food.count
    //	emit (feel + ": ", maxFood + "," + maxCount)

    public static class Reduce1
    extends Reducer<Text,Text, Text, Text> {
       
       // private Text feel = new Text();
        private Text maxFood = new Text();
                
    	int itemCount;
//    	HashMap<Text, IntWritable> foodCount = new HashMap<Text, IntWritable>();
//    	HashMap<Text, HashMap<Text, IntWritable>> feelToFood = new HashMap<Text, HashMap<Text, IntWritable>>();
//    	HashMap<HashMap<Text, Text> ,IntWritable> feelFoodToCount = new HashMap<HashMap<Text, Text> ,IntWritable>();
    	HashMap<Pair<Text, Text> ,IntWritable> feelFoodToCount = new HashMap<Pair<Text, Text> ,IntWritable>();

        public void reduce(Text key, Iterable<Text> foodItems,
                           Context context
                           ) throws IOException, InterruptedException {
            for (Text foodItem : foodItems) {
				System.out.println("feel: " + key + " foodItem: " + foodItem); // testing
//				Text feelFood = new Text("feel: " + key + " foodItem: " + foodItem);
//				Text outputValue = new Text("1");
//				context.write(feelFood, outputValue);
//				HashMap<Text, Text> feelFood = new HashMap<Text, Text>();
				Pair<Text, Text> feelFood = new Pair(key, foodItem);
//				feelFood.put(key,foodItem);
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
            
//          Iterator entries = feelFoodToCount.entrySet().iterator();
//          while (entries.hasNext()) {
//        		Entry thisEntry = (Entry) entries.next();
//        		HashMap<Text,Text> fF = new HashMap<Text,Text>();
//            	fF= (HashMap<Text, Text>) thisEntry.getKey(); 
//            	Iterator entries2 = fF.entrySet().iterator();
//            	Entry thisEntry2 = (Entry) entries.next();
//             	Text k = (Text) thisEntry2.getKey(); //feel - HashMap
//             	Text v = (Text) thisEntry2.getValue(); //Food - HashMap
//             	Text result = new Text("feeling: "  + k + " food: " + v); //feel Food
//             	context.write(result, new Text(thisEntry.getValue().toString()));
//          }

//          int maxCount = 0;
//            Iterator entries = feelFoodToCount.entrySet().iterator();
//            while (entries.hasNext()) {
//            	Entry thisEntry = (Entry) entries.next();
//            	Text k = (Text) thisEntry.getKey(); //feelFood - HashMap
//            	//System.out.println("key(foodItem): " + k);
//            	int v =  Integer.parseInt(thisEntry.getValue().toString()); //ItemCount for specific feelFood
//            	System.out.println("value(foodItemCount): " + v);
//            	if( v >= maxCount)
//            	{
//            		maxCount = v;
//            		maxFood = k;
//            	}		
//            }
//            Text result = new Text(": " + maxFood.toString() + "," + Integer.toString(maxCount));
//            context.write(key, result);
        }

    }
    
    public static class Map2
    extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        		) throws IOException, InterruptedException {
        	
        }
    }
    public static class Reduce2
    extends Reducer<Text,Text, Text, Text> {

    	public void reduce(Text key, Iterable<Text> foodItems,
    			Context context
    			) throws IOException, InterruptedException {

    	}
    }
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

        
        //JOB 2
        Job job2 = Job.getInstance(conf, "job two");
        job2.setJarByClass(dietaryRestrictions.class);
        job2.setMapperClass(Map2.class);
        job2.setCombinerClass(Reduce2.class);
        job2.setReducerClass(Reduce2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("temp"));
        FileOutputFormat.setOutputPath(job2, new Path("output"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        
        //System.exit(job.waitForCompletion(true) ? 0 : 1);

        System.out.println("Job Completed");
      }
    
}







