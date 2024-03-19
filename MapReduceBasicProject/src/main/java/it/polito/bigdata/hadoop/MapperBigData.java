package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
            //get parameter from driver class
            int numberOfGrams = context.getConfiguration().getInt("numberOfGrams", 1);
            // Split each sentence in words. Use whitespace(s) as delimiter 
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] words = value.toString().split("\\s+");
            
            // Iterate over the set of words
            for (int i = 0; i < words.length - 1; i++) {
                // Form a 2-gram by combining the current word and the next word
                String nGram = "";
                for (int j = 0; j < numberOfGrams; j++) {
                    if (i + j >= words.length) {
                        break;
                    }
                    nGram += words[i + j].toLowerCase() + " ";
                }
                System.out.println(nGram);
                context.write(new Text(nGram), new IntWritable(1));
            }
    }
}
