package org.olatunbosun.hadoop;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import com.mongodb.BasicDBObjectBuilder;

import java.io.IOException;
/**
 * @author olulodeolatunbosun
 * @created 25/06/2024/06/2024 - 14:10
 */


public class MaxCityReducer extends Reducer<Text, IntWritable, ObjectId, BSONObject> {

    private Text maxCity = new Text();
    private int maxCount = 0;

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        if (sum > maxCount) {
            maxCount = sum;
            maxCity.set(key);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        BSONObject doc = BasicDBObjectBuilder.start()
                .add("most_affected_city", maxCity.toString())
                .add("number of patients", maxCount).get();

        context.write(new ObjectId(), doc);
    }
}
