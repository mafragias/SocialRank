package gr.tuc.softnet;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DiffMapper1 extends Mapper<Object, Text, Text, Text>{
	public void map(Object key, Text value, Context output) throws IOException,
    InterruptedException {
		String[] parts = value.toString().split("\t");
		String[] rankandfollowing = parts[1].split("!");	// separating rank from following users
		if (parts[0].matches("[0-9]+") && rankandfollowing[0].matches("^([0-9]*.[0-9]+|[0-9]+)$")) {
			output.write(new Text(parts[0]), new Text(rankandfollowing[0]));	// key: user value: rank
		}else {
			System.err.println("Error at DiffMapper1 types");
		}
	}
}
