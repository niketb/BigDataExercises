package spring2018.lab2;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.Iterator;

public class AAReducer  extends Reducer <Text,Text,Text,Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
        
        // TODO: initialize integer sums for each reading frame
    	int frame1Sum = 0;
    	int frame2Sum = 0;
    	int frame3Sum = 0;
        
        // TODO: loop through Iterable values and increment sums for each reading frame
    	Iterator<Text> iterator = values.iterator();
    	while (iterator.hasNext()) 
        { 
    		Text frameNumber = iterator.next();
    		if(frameNumber.toString().equals("1")) {  
    			frame1Sum++;
    		}
    		else if(frameNumber.toString().equals("2")) {
    			frame2Sum++;
    		}
    		else if(frameNumber.toString().equals("3")) {
    			frame3Sum++;
    		}
        } 
       
        // TODO: write the (key, value) pair to the context
        // TODO: consider how to use tabs to format output correctly
    	context.write(key, new Text("\t" + frame1Sum + "\t" + frame2Sum + "\t" + frame3Sum));
	  
   }
}