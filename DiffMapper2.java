package gr.tuc.softnet;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DiffMapper2 extends Mapper<Object, Text, Text, Text>{
	public void map(Object key, Text value, Context output) throws IOException,
    InterruptedException {
		String[] parts = value.toString().split("\t");
		if (parts[0].equals("Diff") && parts[1].matches("^([0-9]*.[0-9]+|[0-9]+)$")) {
			output.write(new Text(parts[0]), new Text(parts[1]));	// key: Diff value: difference 
			// we want all values under one key 
		}else {
			System.err.println("Error at DiffMapper2 types");
		}
	}
}