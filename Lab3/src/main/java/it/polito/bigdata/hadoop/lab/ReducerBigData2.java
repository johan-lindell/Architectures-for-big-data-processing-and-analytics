package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<NullWritable, // Input key type
        Text, // Input value type
        Text, // Output key type
        IntWritable> { // Output value type

    @Override
    protected void reduce(
            NullWritable key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {
        try {
            TopKVector<WordCountWritable> top100 = new TopKVector<WordCountWritable>(100);
            /* Implement the reduce method */
            for (Text value : values) {
                String[] pairAndOccurrences = value.toString().split("\t");
                top100.updateWithNewElement(
                        new WordCountWritable(pairAndOccurrences[0], new Integer(pairAndOccurrences[1])));
            }

            for (WordCountWritable wordCount : top100.getLocalTopK()) {
                context.write(new Text(wordCount.getWord()), new IntWritable(wordCount.getCount()));
            }
        } catch (Exception e) {
            // Log the error or handle it as needed
            e.printStackTrace();
        }
    }
}
