package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    
	@Override
    public void reduce(IntWritable key, Iterable<Text> values,Context output) 
    		throws IOException, InterruptedException {
		double d = 0.15;
		double sum = 0;
		double rank = 0;
		String outValue = "";
		for (Text val:values){
			String valsum = val.toString();
			if((valsum.contains("!"))){ // valsum = !N(i)
				outValue = valsum.substring(1);
			}else{	// valsum = weight
				sum = sum + Double.parseDouble(valsum);
			}
		}
		rank = d + (1-d)*sum;	// calculating rank
		outValue = rank + "!" + outValue;
		output.write(key,new Text(outValue)); // key: user value: newrank!N(i)
	}

}
