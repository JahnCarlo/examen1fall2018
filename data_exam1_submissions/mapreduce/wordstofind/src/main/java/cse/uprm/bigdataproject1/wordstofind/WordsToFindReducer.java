package cse.uprm.bigdataproject1.wordstofind;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordsToFindReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String contents = "";

        for (Text value : values){
            contents += value.toString()+" ";
        }
        context.write(key, new Text(contents));
    }
}