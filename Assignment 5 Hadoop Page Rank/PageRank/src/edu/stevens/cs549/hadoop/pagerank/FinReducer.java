package edu.stevens.cs549.hadoop.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinReducer extends Reducer<DoubleWritable, Text, Text, Text> {
	
	// @Override
	// public void setup(Context context) {
	// 	try {
	// 		super.setup(context);
	// 		URI[] files = context.getCacheFiles();
	// 		// Path path = new Path(files[0]);
	// 		FileSystem fs = FileSystem.get(context.getConfiguration());
	// 		Path path = new Path(files[0].toString());
	// 		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
	// 	} catch (IOException | InterruptedException e) {
	// 		// TODO Auto-generated catch block
	// 		e.printStackTrace();
	// 	}
	// }
	

	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		/* 
		 * TODO: For each value, emit: key:value, value:-rank
		 */
		Iterator<Text> iterator = values.iterator();
		String node;
		while(iterator.hasNext()) {
			node = iterator.next().toString();
			context.write(new Text(node), new Text(String.valueOf(0 - key.get())));
		}

	}
}
