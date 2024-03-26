package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<Text, // Input key type
        Text, // Input value type
        NullWritable, // Output key type
        Text> {// Output value type
    private TopKVector<WordCountWritable> localTop100 = new TopKVector<WordCountWritable>(100);

    protected void map(
            Text key, // Input key type
            Text value, // Input value type
            Context context) throws IOException, InterruptedException {

        try {
            /* Implement the map method */
            // we get the pair and the number of occurrences
            localTop100.updateWithNewElement(
                    new WordCountWritable(key.toString(), new Integer(value.toString())));
        } catch (Exception e) {
            // Log the error or handle it as needed
            System.err.println("Error processing record: " + key.toString() + ", " + value.toString());
            e.printStackTrace();
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (WordCountWritable wordCount : localTop100.getLocalTopK()) {
            context.write(NullWritable.get(), new Text(wordCount.getWord() + "\t" + wordCount.getCount()));
        }

    }
}
