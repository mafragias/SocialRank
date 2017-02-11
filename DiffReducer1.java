package gr.tuc.softnet;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DiffReducer1 extends Reducer<Text, Text, Text, Text>{
	@Override
    public void reduce(Text key, Iterable<Text> values,Context output) 
    		throws IOException, InterruptedException {
		double diff=0;
		for (Text val:values){ // 2 loops
			diff = Math.abs(diff - Double.parseDouble(val.toString()));
		}
		output.write(new Text("Diff"), new Text(diff+"")); // key: Diff value: diff=|rank1-rank2|
	}
}
