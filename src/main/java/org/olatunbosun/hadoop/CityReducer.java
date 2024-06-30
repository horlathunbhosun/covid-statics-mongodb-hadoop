package org.olatunbosun.hadoop;

import com.mongodb.BasicDBObjectBuilder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

import java.io.IOException;

/**
 * @author olulodeolatunbosun
 * @created 25/06/2024/06/2024 - 12:52
 */


public class CityReducer extends Reducer<Text, IntWritable, ObjectId, BSONObject> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        BSONObject doc = BasicDBObjectBuilder.start()
                .add("city", key.toString())
                .add("number of patients", sum).get();

        context.write(new ObjectId(), doc);
    }
}