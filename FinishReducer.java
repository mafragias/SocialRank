package gr.tuc.softnet;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinishReducer extends Reducer<DoubleWritable, Text, Text, Text> {
   
	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values,Context output) 
    		throws IOException, InterruptedException {
		for(Text val:values){
			String k = key.toString().substring(1);	// keeping the positive rank
			Double toBeTruncated = new Double(Double.parseDouble(k));	// Precision to 3 digits 
			Double truncatedDouble = new BigDecimal(toBeTruncated).setScale(3, BigDecimal.ROUND_HALF_EVEN).doubleValue();
			output.write(val,new Text(truncatedDouble+""));	// key: user value: rank
		}
	}
    
}
