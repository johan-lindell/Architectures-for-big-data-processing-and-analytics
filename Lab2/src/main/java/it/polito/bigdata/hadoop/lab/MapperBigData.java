package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.polito.bigdata.hadoop.lab.DriverBigData.COUNTER;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
            
            //get conf parameter
            String filterString = context.getConfiguration().get("filterString");
                    
    		/* Implement the map method */ 
            String[] pair = value.toString().split("\t");

            String word = pair[0];
            int count = Integer.parseInt(pair[1]);

            if (word.startsWith(filterString)) {
                context.getCounter(COUNTER.WORDS_COUNTED).increment(1);
                context.write(new Text(word), new IntWritable(count));
            } else {
                context.getCounter(COUNTER.WORDS_DISCARDED).increment(1);
            }
    }
}
