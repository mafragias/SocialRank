package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitMapper extends Mapper<Object, Text, IntWritable, Text> {
	
    @Override
    public void map(Object key, Text value, Context output) throws IOException,
            InterruptedException {
		String[] parts = value.toString().split("\t");
		if (parts[0].matches("[0-9]+") && parts[1].matches("[0-9]+")) { // Makes sure the vertices are numbers only
			//formating keys and values to type Text 
			IntWritable x = new IntWritable(Integer.parseInt(parts[0])); 
			Text y = new Text(parts[1]);
			//sending keys and values to the reducers
			output.write(x, y); // gets all accounts that user x follows 
			// input-> key : userX (follows) value: userY
			// output-> key : userX (follows) value: userY1,userY2,...
		}else {
			System.err.println("Error at InitMapper types");
		}
    }
}
	
