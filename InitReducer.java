package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InitReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    
	@Override
    public void reduce(IntWritable key, Iterable<Text> values,Context output) 
    		throws IOException, InterruptedException {
		String outValue = "";
		for (Text val : values) { // for all the values in the same key
			outValue = outValue.concat(","+val.toString()); // find N(i)
		}
		outValue = ("1!").concat(outValue.substring(1));	// initiate rank=1 and separate it from N(i)
		
		output.write(key,new Text(outValue));	// key = user values = rank!N(i) 
	}
    
}
