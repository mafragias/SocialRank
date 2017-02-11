package gr.tuc.softnet;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DiffReducer2 extends Reducer<Text, Text, Text, Text>{
	@Override
    public void reduce(Text key, Iterable<Text> values,Context output) 
    		throws IOException, InterruptedException {
		double max = Double.NEGATIVE_INFINITY;
		for (Text val:values){	// calculating the mean difference
			
			if(Double.parseDouble(val.toString())>max){
				max = Double.parseDouble(val.toString()) ;
			}
			
		}
		output.write(new Text("Max"), new Text(max+""));	// key: Max value: max difference
	}
}