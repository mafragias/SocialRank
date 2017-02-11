package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IterMapper  extends Mapper<Object, Text, IntWritable, Text>{
	@Override
    public void map(Object key, Text value, Context output) throws IOException, InterruptedException{
		String[] parts = value.toString().split("\t");
		if (parts[0].matches("[0-9]+") && parts[1].matches("^([0-9]*.[0-9]+|[0-9]+)!(([0-9,]+[0-9])|[0-9]|)$")) {
			String[] rankandfollowing = parts[1].split("!");	//splits rank from the following users
			if(rankandfollowing.length!=1){
				String[] following = rankandfollowing[1].split(",");	//splits following users
				double rank = Double.parseDouble(rankandfollowing[0]); 
				double numoffollowing = following.length;
			
				double weight = rank/numoffollowing;	// weight each follower B(i) -> rank(i)/|N(i)|
			
				for(int i=0;i<following.length;i++)
					output.write(new IntWritable(Integer.parseInt(following[i])), new Text(weight+"") );	//key: follower value: weight
			
				output.write(new IntWritable(Integer.parseInt(parts[0])), new Text("!"+rankandfollowing[1]));//key: user (follows) values: !N(i)
			}else{
				output.write(new IntWritable(Integer.parseInt(parts[0])),new Text("0"));	//if a user doesn't follow anyone | key: user value: 0
			}
		}else {
			System.err.println("Error at IterMapper types");
		}
	}

}
