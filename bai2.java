import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai2 {

    // mapper for rating_*.txt
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
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
                String movieId = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());

                movieIdKey.set(movieId);
                ratingValue.set(String.format("RATING:%.2f", rating));

                context.write(movieIdKey, ratingValue);

            } catch (NumberFormatException e) {
                // ignoring all irrelevant records
            }
        }
    }

    // mapper for movies.txt
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text genreValue = new Text();

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
                String genres = parts[2].trim(); // Genres column

                movieIdKey.set(movieId);
                genreValue.set(String.format("GENRES:%s", genres));

                context.write(movieIdKey, genreValue);

            } catch (Exception e) {
                // ignoring all irrelevant records
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Collect ratings and genres for this movie
            List<Double> ratings = new ArrayList<>();
            String genres = "";
            
            for(Text val : values) {
                String value = val.toString();
                if (value.startsWith("RATING:")) {
                    String ratingStr = value.replace("RATING:", "");
                    ratings.add(Double.parseDouble(ratingStr));
                } else if (value.startsWith("GENRES:")) {
                    genres = value.replace("GENRES:", "");
                }
            }
            
            // If we have both ratings and genres, emit for each genre
            if (!ratings.isEmpty() && !genres.isEmpty()) {
                String[] genreArray = genres.split("\\|");
                
                for (String genre : genreArray) {
                    genre = genre.trim();
                    
                    // Emit each rating individually for this genre
                    for (Double rating : ratings) {
                        outputKey.set(genre);
                        outputValue.set(String.format("%.2f", rating));
                        context.write(outputKey, outputValue);
                    }
                }
            }
        }
    }

    // Simple identity mapper for second job
    public static class IdentityMapper extends Mapper<Object, Text, Text, Text> {
        private Text genreKey = new Text();
        private Text ratingValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split("\t");
            if (parts.length >= 2) {
                String genre = parts[0].trim();
                String rating = parts[1].trim();
                
                genreKey.set(genre);
                ratingValue.set(rating);
                context.write(genreKey, ratingValue);
            }
        }
    }

    // Final reducer to aggregate ratings by genre
    public static class FinalAggregateReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String genre = key.toString();
            double sum = 0.0;
            int count = 0;
            
            for(Text val : values) {
                String ratingStr = val.toString();
                try {
                    double rating = Double.parseDouble(ratingStr);
                    sum += rating;
                    count++;
                } catch (NumberFormatException e) {
                    // ignore invalid ratings
                }
            }
            
            if (count > 0) {
                double avg = sum / count;
                outputKey.set(genre);
                outputValue.set(String.format("Avg: %.2f, Count: %d", avg, count));
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        
        // Job 1: Map movie-genre relationships and emit individual ratings per genre
        Job job1 = Job.getInstance(conf, "Genre Rating Mapping");

        job1.setJarByClass(bai2.class);
        job1.setReducerClass(GenreReducer.class);
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
            MovieMapper.class
        );
        
        Path intermediateOutput = new Path(args[2] + "_temp");
        FileOutputFormat.setOutputPath(job1, intermediateOutput);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2: Aggregate final results by genre
        Job job2 = Job.getInstance(conf, "Genre Rating Aggregation");
        job2.setJarByClass(bai2.class);
        job2.setMapperClass(IdentityMapper.class);
        job2.setReducerClass(FinalAggregateReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job2, intermediateOutput);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}