package p;

import java.io.IOException;
import java.io.PrintStream;
import java.util.StringTokenizer;
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
	
    public static class TokenizerMapper
    extends Mapper<Object, Text, Text, Text>{

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm");

//        private Text word = new Text();
        String word = "";
//        private Text feeling = new Text();
        String feeling = "";
        //    private IntWritable feelingDate = new IntWritable();
        Date feelingDate = new Date();
//        private Text food = new Text();
        String food = "";
        //    private IntWritable foodDate = new IntWritable();
        Date foodDate = new Date();
        private Text feel = new Text();
        private Text foodItem = new Text();
        //    Map<Text,IntWritable> feelingList = new HashMap<Text,IntWritable>();
        Map<Text,Date> feelingList = new HashMap<Text,Date>();


        public void map(Object key, Text value, Context context
        		) throws IOException, InterruptedException {
        	//StringTokenizer itr = new StringTokenizer(value.toString());
        	//while (itr.hasMoreTokens()) {
        	//word.set(itr.nextToken()); // ** error here loops and never reaches if statement
        	//            	System.out.println("Word: " + word);

        	String[] arr = value.toString().split(" ");    

        	int i = 0;
        	for(; i < arr.length; i++) {

        		word = arr[i];
        		i++;
        		System.out.println("Word: " + word);
        		if(word.equals("feel"))
        		{
        			feeling = arr[i];
        			i++;
        			i++; //increment twice b/c douple space in input being counted as a token
        			System.out.println("feeling: " + feeling);
        			try {
        				String dateWithTime = arr[i]; //yyyy/MM/dd
        				i++; 
        				System.out.println("feelDate: " + dateWithTime);
        				dateWithTime += " ";
        				dateWithTime += arr[i]; //HH:mm
        				i++;
        				System.out.println("feelDateWithTime: " + dateWithTime);
        				feelingDate = dateFormat.parse(dateWithTime);
        			} catch (ParseException e) {
        				e.printStackTrace();
        			}
        			//        	feelingDate =  new IntWritable(Integer.parseInt(itr.nextToken()));
        			//word.set(itr.nextToken()); //just to discard token
        			//System.out.println("tokenDiscard: " + word);
        			feelingList.put(new Text(feeling), feelingDate);
        		}
        		else if(word.equals("food"))
        		{
        			food = arr[i];
        			i++;
        			i++;// increment twice b/c douple space in input being counted as a token
        			System.out.println("food: " + food);
        			//        	foodDate =  new IntWritable(Integer.parseInt(itr.nextToken()));
        			try {
        				String dateWithTime  = arr[i];
        				i++;
        				System.out.println("foodDate: " + dateWithTime);
        				dateWithTime += " ";
        				dateWithTime += arr[i];
        				i++;
        				System.out.println("foodDateWithTime: " + dateWithTime);
        				foodDate = dateFormat.parse(dateWithTime);
        			} catch (ParseException e) {
        				e.printStackTrace();
        			}
        			//                    word.set(itr.nextToken()); //just to discard token
        			//                	System.out.println("tokenDiscard: " + word);

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
        	} //} 
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
    extends Reducer<Text,Text,Text,Text> {
       
       // private Text feel = new Text();
        private Text maxFood = new Text();
        private int maxCount = 0;
        
        private Map<Text,IntWritable> foodItemCount = new HashMap<Text,IntWritable>();

        public void reduce(Text key, Iterable<Text> foodItems,
                           Context context
                           ) throws IOException, InterruptedException {
            for (Text foodItem : foodItems) {
            	if (foodItemCount.get(foodItem) != null)
            	{
            		int itemCount = (foodItemCount.get(foodItem)).get(); //get foodItemCount
            		itemCount++;
            		IntWritable newItemCount = new IntWritable();
            		newItemCount.set(itemCount);
                    foodItemCount.put(foodItem, newItemCount);
            	}
            }
            Iterator entries = foodItemCount.entrySet().iterator();
            while (entries.hasNext()) {
            	Entry thisEntry = (Entry) entries.next();
            	Text k = (Text) thisEntry.getKey(); //foodItem
            	int v =  Integer.parseInt(thisEntry.getValue().toString()); //foodItemCount
            	if( v >= maxCount)
            	{
            		maxCount = v;
            		maxFood = k;
            	}		
            }
            
            Text result = new Text(": " + maxFood.toString() + "," + Integer.toString(maxCount));
            context.write(key, result);
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
        //job.setOutputKeyClass(IntWritable.class); // changed from Text.class
        //job.setOutputValueClass(IntWritable.class);
        
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

    



