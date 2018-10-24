package cse.uprm.bigdataproject1.wordstofind;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

public class WordsToFindMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String tweet = status.getText().toUpperCase();
            if (tweet.contains("FLU")){
                context.write(new Text("FLU"), new Text(Long.toString(status.getId())));
            }
            if (tweet.contains("ZIKA")){
                context.write(new Text("ZIKA"), new Text(Long.toString(status.getId())));
            }
            if (tweet.contains("DIARRHEA")){
                context.write(new Text("DIARRHEA"), new Text(Long.toString(status.getId())));
            }
            if (tweet.contains("EBOLA")){
                context.write(new Text("EBOLA"), new Text(Long.toString(status.getId())));
            }
            if (tweet.contains("SWAMP")){
                context.write(new Text("SWAMP"), new Text(Long.toString(status.getId())));
            }
            if (tweet.contains("CHANGE")){
                context.write(new Text("CHANGE"), new Text(Long.toString(status.getId())));
            }

        }
        catch(TwitterException e){

        }

    }
}