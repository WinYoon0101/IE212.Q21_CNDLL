import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai3 {

    // mapper for rating_*.txt
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text userIdKey = new Text();
        private Text ratingValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");

            if (parts.length < 4) {
                return;
            }

            try {
                String userId = parts[0].trim();
                String movieId = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());

                userIdKey.set(userId);
                ratingValue.set(String.format("RATING:%s:%.2f", movieId, rating));

                context.write(userIdKey, ratingValue);

            } catch (NumberFormatException e) {
                // ignoring all irrelevant records
            }
        }
    }

    // mapper for users.txt
    public static class UserMapper extends Mapper<Object, Text, Text, Text> {
        private Text userIdKey = new Text();
        private Text genderValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");

            if (parts.length < 3) {
                return;
            }

            try {
                String userId = parts[0].trim();
                String gender = parts[1].trim(); // Gender column

                userIdKey.set(userId);
                genderValue.set(String.format("GENDER:%s", gender));

                context.write(userIdKey, genderValue);

            } catch (Exception e) {
                // ignoring all irrelevant records
            }
        }
    }

    // mapper for movies.txt
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text movieNameValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");

            if (parts.length < 3) {
                return;
            }

            try {
                String movieId = parts[0].trim();
                String movieName = parts[1].trim();

                movieIdKey.set(movieId);
                movieNameValue.set(String.format("MOVIE:%s", movieName));

                context.write(movieIdKey, movieNameValue);

            } catch (NumberFormatException e) {
                // ignoring all irrelevant records
            }
        }
    }

    // First reducer: Join users with ratings to get gender info for each rating
    public static class UserRatingReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Collect ratings and gender for this user
            String gender = "";
            java.util.List<String> movieRatings = new java.util.ArrayList<>();
            
            for(Text val : values) {
                String value = val.toString();
                if (value.startsWith("RATING:")) {
                    // Format: RATING:movieId:rating
                    movieRatings.add(value.replace("RATING:", ""));
                } else if (value.startsWith("GENDER:")) {
                    gender = value.replace("GENDER:", "");
                }
            }
            
            // If we have both gender and ratings, emit for each movie
            if (!gender.isEmpty() && !movieRatings.isEmpty()) {
                for (String movieRating : movieRatings) {
                    String[] parts = movieRating.split(":");
                    if (parts.length == 2) {
                        String movieId = parts[0];
                        String rating = parts[1];
                        
                        outputKey.set(movieId);
                        outputValue.set(String.format("RATING_GENDER:%s:%s", gender, rating));
                        context.write(outputKey, outputValue);
                    }
                }
            }
        }
    }

    // Simple identity mapper for intermediate data
    public static class IdentityMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text value = new Text();

        public void map(Object key, Text inputValue, Context context) throws IOException, InterruptedException {
            String line = inputValue.toString().trim();
            
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split("\t");
            if (parts.length >= 2) {
                String movieId = parts[0].trim();
                String ratingGender = parts[1].trim();
                
                movieIdKey.set(movieId);
                value.set(ratingGender);
                context.write(movieIdKey, value);
            }
        }
    }

    // Second reducer: Join with movies and aggregate by gender
    public static class MovieGenderReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieName = "";
            double maleSum = 0.0, femaleSum = 0.0;
            int maleCount = 0, femaleCount = 0;
            
            for(Text val : values) {
                String value = val.toString();
                if (value.startsWith("MOVIE:")) {
                    movieName = value.replace("MOVIE:", "");
                } else if (value.startsWith("RATING_GENDER:")) {
                    String ratingGender = value.replace("RATING_GENDER:", "");
                    String[] parts = ratingGender.split(":");
                    if (parts.length == 2) {
                        String gender = parts[0];
                        double rating = Double.parseDouble(parts[1]);
                        
                        if ("M".equals(gender)) {
                            maleSum += rating;
                            maleCount++;
                        } else if ("F".equals(gender)) {
                            femaleSum += rating;
                            femaleCount++;
                        }
                    }
                }
            }
            
            if (!movieName.isEmpty() && (maleCount > 0 || femaleCount > 0)) {
                String maleAvg = maleCount > 0 ? String.format("%.2f", maleSum / maleCount) : "NA";
                String femaleAvg = femaleCount > 0 ? String.format("%.2f", femaleSum / femaleCount) : "NA";
                
                outputKey.set(movieName);
                outputValue.set(String.format("Male: %s, Female: %s", maleAvg, femaleAvg));
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        
        // Job 1: Join users with ratings to get gender info for each rating
        Job job1 = Job.getInstance(conf, "User Rating Gender Join");

        job1.setJarByClass(bai3.class);
        job1.setReducerClass(UserRatingReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(
            job1, 
            new Path(args[0]), 
            TextInputFormat.class, 
            RatingMapper.class
        );
        MultipleInputs.addInputPath(
            job1, 
            new Path(args[1]),
            TextInputFormat.class, 
            UserMapper.class
        );
        
        Path intermediateOutput = new Path(args[3] + "_temp");
        FileOutputFormat.setOutputPath(job1, intermediateOutput);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2: Join with movies and aggregate by gender
        Job job2 = Job.getInstance(conf, "Movie Gender Rating Analysis");
        job2.setJarByClass(bai3.class);
        job2.setMapperClass(IdentityMapper.class);
        job2.setReducerClass(MovieGenderReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(
            job2, 
            intermediateOutput, 
            TextInputFormat.class, 
            IdentityMapper.class
        );
        MultipleInputs.addInputPath(
            job2, 
            new Path(args[2]),
            TextInputFormat.class, 
            MovieMapper.class
        );
        
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}