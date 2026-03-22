import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai4 {

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
        private Text ageGroupValue = new Text();

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
                int age = Integer.parseInt(parts[2].trim()); // Age column
                
                // Determine age group
                String ageGroup;
                if (age <= 18) {
                    ageGroup = "0-18";
                } else if (age <= 35) {
                    ageGroup = "18-35";
                } else if (age <= 50) {
                    ageGroup = "35-50";
                } else {
                    ageGroup = "50+";
                }

                userIdKey.set(userId);
                ageGroupValue.set(String.format("AGEGROUP:%s", ageGroup));

                context.write(userIdKey, ageGroupValue);

            } catch (NumberFormatException e) {
                // ignoring all irrelevant records
            }
        }
    }

    // First reducer: Join users with ratings to get age group info for each rating
    public static class UserRatingReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Collect ratings and age group for this user
            String ageGroup = "";
            java.util.List<String> movieRatings = new java.util.ArrayList<>();
            
            for(Text val : values) {
                String value = val.toString();
                if (value.startsWith("RATING:")) {
                    // Format: RATING:movieId:rating
                    movieRatings.add(value.replace("RATING:", ""));
                } else if (value.startsWith("AGEGROUP:")) {
                    ageGroup = value.replace("AGEGROUP:", "");
                }
            }
            
            // If we have both age group and ratings, emit for each movie
            if (!ageGroup.isEmpty() && !movieRatings.isEmpty()) {
                for (String movieRating : movieRatings) {
                    String[] parts = movieRating.split(":");
                    if (parts.length == 2) {
                        String movieId = parts[0];
                        String rating = parts[1];
                        
                        outputKey.set(movieId);
                        outputValue.set(String.format("RATING_AGE:%s:%s", ageGroup, rating));
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
                String ratingAge = parts[1].trim();
                
                movieIdKey.set(movieId);
                value.set(ratingAge);
                context.write(movieIdKey, value);
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

    // Second reducer: Join with movies and aggregate by age group
    public static class MovieAgeReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieName = "";
            
            // Age group statistics
            double sum_0_18 = 0.0, sum_18_35 = 0.0, sum_35_50 = 0.0, sum_50_plus = 0.0;
            int count_0_18 = 0, count_18_35 = 0, count_35_50 = 0, count_50_plus = 0;
            
            for(Text val : values) {
                String value = val.toString();
                if (value.startsWith("MOVIE:")) {
                    movieName = value.replace("MOVIE:", "");
                } else if (value.startsWith("RATING_AGE:")) {
                    String ratingAge = value.replace("RATING_AGE:", "");
                    String[] parts = ratingAge.split(":");
                    if (parts.length == 2) {
                        String ageGroup = parts[0];
                        double rating = Double.parseDouble(parts[1]);
                        
                        switch (ageGroup) {
                            case "0-18":
                                sum_0_18 += rating;
                                count_0_18++;
                                break;
                            case "18-35":
                                sum_18_35 += rating;
                                count_18_35++;
                                break;
                            case "35-50":
                                sum_35_50 += rating;
                                count_35_50++;
                                break;
                            case "50+":
                                sum_50_plus += rating;
                                count_50_plus++;
                                break;
                        }
                    }
                }
            }
            
            if (!movieName.isEmpty() && (count_0_18 + count_18_35 + count_35_50 + count_50_plus > 0)) {
                String avg_0_18 = count_0_18 > 0 ? String.format("%.2f", sum_0_18 / count_0_18) : "NA";
                String avg_18_35 = count_18_35 > 0 ? String.format("%.2f", sum_18_35 / count_18_35) : "NA";
                String avg_35_50 = count_35_50 > 0 ? String.format("%.2f", sum_35_50 / count_35_50) : "NA";
                String avg_50_plus = count_50_plus > 0 ? String.format("%.2f", sum_50_plus / count_50_plus) : "NA";
                
                outputKey.set(movieName);
                outputValue.set(String.format("0-18: %s 18-35: %s 35-50: %s 50+: %s", 
                    avg_0_18, avg_18_35, avg_35_50, avg_50_plus));
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        
        // Job 1: Join users with ratings to get age group info for each rating
        Job job1 = Job.getInstance(conf, "User Rating Age Group Join");

        job1.setJarByClass(bai4.class);
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

        // Job 2: Join with movies and aggregate by age group
        Job job2 = Job.getInstance(conf, "Movie Age Group Rating Analysis");
        job2.setJarByClass(bai4.class);
        job2.setMapperClass(IdentityMapper.class);
        job2.setReducerClass(MovieAgeReducer.class);
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