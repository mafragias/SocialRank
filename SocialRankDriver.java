package gr.tuc.softnet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SocialRankDriver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SocialRankDriver(), args);
        System.exit(res);       
    }
	
	@Override
    public int run(String[] args) throws Exception {
		
        if (args.length == 4) {
        	if(args[0].equals("init")){
            	init(args[1],args[2],Integer.parseInt(args[3]));          	
            }else if(args[0].equals("iter")){
            	iter(args[1],args[2],Integer.parseInt(args[3]));
            }else if(args[0].equals("finish")){
            	finish(args[1],args[2],Integer.parseInt(args[3]));
            }
        }else if (args.length == 5 && args[0].equals("diff")){
        	diff(args[1],args[2],args[3],Integer.parseInt(args[4]));
        }else if (args.length == 8 && args[0].equals("composite")){
        	long startcomposite = System.currentTimeMillis();
        	composite(args[1],args[2],args[3],args[4],args[5],Double.parseDouble(args[6]),Integer.parseInt(args[7]));
        	long endcomposite = System.currentTimeMillis()-startcomposite;
        	System.out.println("Composite Execution Time = " + (endcomposite*0.001)/60 + " min."); 
        }else{
        	if (args.length == 4){
        		System.out.println("usage: [function name] [input] [output] [numofreducers]");
        	}else if(args.length == 5){
        		System.out.println("usage: [function name] [input1] [input2] [output] [numofreducers]");
        	}else if(args.length == 8){
        		System.out.println("usage: [function name] [input] [output] [interm1] [interm2] [diff] [threshold] [numofreducers]");
        	}
            System.exit(-1);
        }

        return 0;
    }
	
	public void init(String inputDir,String outputDir,int numofreducers) throws ClassNotFoundException, IOException, InterruptedException{
		Job job1 = Job.getInstance(new Configuration());
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(InitMapper.class);
        job1.setReducerClass(InitReducer.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job1, new Path(inputDir));
        deletingDir(outputDir);
        FileOutputFormat.setOutputPath(job1, new Path(outputDir));

        job1.setJarByClass(SocialRankDriver.class);
        job1.setNumReduceTasks(numofreducers);
        
        job1.submit();
        job1.waitForCompletion(true);
        
	}
	
	public void iter(String inputDir,String outputDir,int numofreducers) throws ClassNotFoundException, IOException, InterruptedException{
		Job job2 = Job.getInstance(new Configuration());
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(IterMapper.class);
        job2.setReducerClass(IterReducer.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2, new Path(inputDir));
        deletingDir(outputDir);
        FileOutputFormat.setOutputPath(job2, new Path(outputDir));

        job2.setJarByClass(SocialRankDriver.class);
        job2.setNumReduceTasks(numofreducers);
        
        job2.submit();
        job2.waitForCompletion(true);
	}
	
	public void diff(String inputDir1,String inputDir2,String outputDir,int numofreducers) throws ClassNotFoundException, IOException, InterruptedException{
		Job job3 = Job.getInstance(new Configuration());
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setMapperClass(DiffMapper1.class);
        job3.setReducerClass(DiffReducer1.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job3, new Path(inputDir1), new Path(inputDir2));
        deletingDir("/user/hue/socialrank/outputdiff1");
        FileOutputFormat.setOutputPath(job3, new Path("/user/hue/socialrank/outputdiff1"));

        job3.setJarByClass(SocialRankDriver.class);
        job3.setNumReduceTasks(numofreducers);
        
        job3.submit();
        
        if(job3.waitForCompletion(true)){
        	Job job4 = Job.getInstance(new Configuration());
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(Text.class);

            job4.setMapperClass(DiffMapper2.class);
            job4.setReducerClass(DiffReducer2.class);

            job4.setInputFormatClass(TextInputFormat.class);
            job4.setOutputFormatClass(TextOutputFormat.class);
            
            FileInputFormat.setInputPaths(job4, new Path("/user/hue/socialrank/outputdiff1"));
            deletingDir(outputDir);
            FileOutputFormat.setOutputPath(job4, new Path(outputDir));

            job4.setJarByClass(SocialRankDriver.class);
            job4.setNumReduceTasks(numofreducers);
            
            job4.submit();
            job4.waitForCompletion(true);
        }
        
	}
	
	public void finish(String inputDir,String outputDir,int numofreducers) throws ClassNotFoundException, IOException, InterruptedException{
		Job job5 = Job.getInstance(new Configuration());
		job5.setMapOutputKeyClass(DoubleWritable.class);
        job5.setMapOutputValueClass(Text.class);
		job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        job5.setMapperClass(FinishMapper.class);
        job5.setReducerClass(FinishReducer.class);

        job5.setInputFormatClass(TextInputFormat.class);
        job5.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job5, new Path(inputDir));
        deletingDir(outputDir);
        FileOutputFormat.setOutputPath(job5, new Path(outputDir));

        job5.setJarByClass(SocialRankDriver.class);
        job5.setNumReduceTasks(numofreducers);
        
        job5.submit();
        job5.waitForCompletion(true);
	}
	
	public void composite(String inputDir,String outputDir,String intermDir1,String intermDir2,String diffDir, double threshold,int numofreducers) throws ClassNotFoundException, IOException, InterruptedException{
		double difference=Double.POSITIVE_INFINITY;	//we get this value from the output of diff function
		System.out.println("Entering INIT");
		init(inputDir,intermDir1,numofreducers);	//initialize data to : user	rank!following1,following2,...
		System.out.println("Exiting INIT");
		
		int counter=1;
		int countiterations=1;
		while(true){
			System.out.println("Entering ITER");
			iter(intermDir1,intermDir2,numofreducers);
			System.out.println("Exiting ITER");
			if (counter==3){//calculating difference every 3 iterations
				System.out.println("Entering DIFF");
				diff(intermDir1,intermDir2,diffDir,numofreducers);
				System.out.println("Exiting DIFF");
				difference = readmaxfromdiffDir(diffDir);
				System.out.println("max Difference = " + difference);
				if(difference<threshold){
					System.out.println("Achieved Threshold");
					break;
				}
				counter=0;
			}
			counter++;
			String temp = intermDir1;
			intermDir1=intermDir2;
			intermDir2=temp;
			countiterations++;
		}
		System.out.println("Entering FINISH");
		finish(intermDir2,outputDir,numofreducers);
		System.out.println("Exiting FINISH");
		System.out.println("Rank Achived on "+ countiterations+ " iterations");
	}
	
	public void deletingDir(String path) throws IOException{
		Configuration conf = new Configuration();
		
		Path dir = new Path(path);
		FileSystem fs = FileSystem.get(conf);

		// delete existing directory
		if (fs.exists(dir)) {
		    fs.delete(dir, true);
		}
	}
	
	public double readmaxfromdiffDir(String dir) throws IOException{
		double max=0;
		//reads diffDir and return max
	    Configuration conf = new Configuration();
	    Path path = new Path(dir+"/part-r-00000");
	    FileSystem fs = FileSystem.get(conf);
	    if(fs.exists(path)){
		    FSDataInputStream inputStream = new FSDataInputStream(fs.open(path));
		    BufferedReader d = new BufferedReader(new InputStreamReader(inputStream));
			String[] diffmax = d.readLine().split("\t");
			max = Double.parseDouble(diffmax[1]);
		    d.close();
	    }
	    return max;
	}
}
