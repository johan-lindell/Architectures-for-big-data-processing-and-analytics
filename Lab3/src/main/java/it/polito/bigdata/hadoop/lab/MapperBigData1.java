package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */
            String[] items = value.toString().split(",");
            String[] itemsExceptFirst = Arrays.copyOfRange(items, 1, items.length);
            for (int i = 0; i < itemsExceptFirst.length; i++) {
                for (int j = i + 1; j < itemsExceptFirst.length; j++) {
                    String pair = itemsExceptFirst[i] + "," + itemsExceptFirst[j];
                    context.write(new Text(pair), new IntWritable(1));
                }
            }
    }
}
