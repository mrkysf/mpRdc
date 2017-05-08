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

public class WordCount {
	
	public class Pair<U, V> {

	    private U first;

	    private V second;

	public Pair(U first, V second) {

	 this.first = first;
	 this.second = second;
		} 
	}
	
    public static class TokenizerMapper
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

    public static class IntSumReducer
    extends Reducer<Text,Text, Text, Text> {
       
       // private Text feel = new Text();
        private Text maxFood = new Text();
                
    	int itemCount;
//    	HashMap<Text, IntWritable> foodCount = new HashMap<Text, IntWritable>();
//    	HashMap<Text, HashMap<Text, IntWritable>> feelToFood = new HashMap<Text, HashMap<Text, IntWritable>>();
    	HashMap<HashMap<Text, Text> ,IntWritable> feelFoodToCount = new HashMap<HashMap<Text, Text> ,IntWritable>();

        public void reduce(Text key, Iterable<Text> foodItems,
                           Context context
                           ) throws IOException, InterruptedException {
            for (Text foodItem : foodItems) {
				System.out.println("feel: " + key + " foodItem: " + foodItem); // testing
				Text feelFood = new Text("feel: " + key + " foodItem: " + foodItem);
				Text outputValue = new Text("1");
				context.write(feelFood, outputValue);
//				HashMap<Text, Text> feelFood = new HashMap<Text, Text>();
//				feelFood.put(key,foodItem);
//				if(feelFoodToCount.get(feelFood) != null)
//				{
//					itemCount = feelFoodToCount.get(feelFood).get();
//					//will replace old value
//					feelFoodToCount.remove(feelFood);
//					System.out.println("itemCount: " + itemCount);
//				}
//				else
//				{
//					itemCount = 0;
//					System.out.println("itemCount: " + itemCount);
//				}
//				itemCount++;
//				System.out.println("newItemCount: " + itemCount); // testing
//				feelFoodToCount.put(feelFood, new IntWritable(itemCount));
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
    
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("6234", "hdfs://localhost:9000");
        
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        
        //new
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //new
       
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        
   
        /*
        Job job2 = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Reverse.class);
        job.setReducerClass(Identity.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("output-2"));
        FileOutputFormat.setOutputPath(job2, new Path("output-3"));
        */
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("Job Completed");
      }
    
}

//    public static class Reverse
//  		extends Mapper<Text, IntWritable, IntWritable, Text> {
//            public void map(Text key, IntWritable value, Context context
//                            ) throws IOException, InterruptedException {
//                context.write(value, key);
//            }
//        }
//
//    public static class Identity
//  		extends Reducer<IntWritable, Text, IntWritable, Text> {
//            static int count = 0;
//
//            public void reduce(IntWritable key, Iterable<Text> values,
//                               Context context) throws IOException, InterruptedException {
//                Iterator<Text> it = values.iterator();
//                while (count < 5 && it.hasNext()) {
//                    context.write(key, it.next());
//                    count++;
//                }
//            }
//        }





