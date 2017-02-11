package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinishMapper extends Mapper<Object, Text, DoubleWritable, Text> {
	
    @Override
    public void map(Object key, Text value, Context output) throws IOException,
            InterruptedException {
    	String[] parts = value.toString().split("\t");
		if (parts[0].matches("[0-9]+") && parts[1].matches("^([0-9]*.[0-9]+|[0-9]+)!(([0-9,]+[0-9])|[0-9]|)$")) {
			String[] rankandfollowing = parts[1].split("!");	// keeping just the ranks
			double k =  -Double.parseDouble(rankandfollowing[0]);	// setting to negative double to get from max to min sorting
			output.write(new DoubleWritable(k),new Text(parts[0]));	// key: -rank value: user
		}else {
			System.err.println("Error at FinishMapper types");
		}
    }
}
