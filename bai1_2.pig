
-- 1. Load data
raw_reviews = LOAD '/mnt/hgfs/LAB2/hotel-review.csv' USING PigStorage(';') AS (id:chararray, comment:chararray, category:chararray, aspect:chararray, sentiment:chararray);
stopwords = LOAD '/mnt/hgfs/LAB2/stopwords.txt' AS (sw:chararray);

-- Lọc dòng trống
reviews = FILTER raw_reviews BY id IS NOT NULL AND id != '';

-- BÀI 1: Làm sạch dữ liệu (Lowercase, Tokenize, Remove Stopwords)
words = FOREACH reviews GENERATE category, aspect, sentiment, FLATTEN(TOKENIZE(REPLACE(LOWER(comment), '[\\p{Punct}]', ' '))) AS word;
valid_words = FILTER words BY word IS NOT NULL AND TRIM(word) != '';

joined_words = JOIN valid_words BY word LEFT OUTER, stopwords BY sw;
cleaned_data = FILTER joined_words BY stopwords::sw IS NULL;

final_words = FOREACH cleaned_data GENERATE valid_words::category AS category, valid_words::aspect AS aspect, valid_words::sentiment AS sentiment, valid_words::word AS word;

-- Lưu kết quả Bài 1 
rmf /home/duong/bai1_results;
STORE final_words INTO '/home/duong/bai1_results' USING PigStorage(',');

-- BÀI 2: Thống kê tần số tất cả các từ và tìm Top 5
word_group = GROUP final_words BY word;
word_count = FOREACH word_group GENERATE group AS word, COUNT(final_words) AS freq;
word_count_ordered = ORDER word_count BY freq DESC;


rmf /home/duong/bai2_tat_ca_tu;
STORE word_count_ordered INTO '/home/duong/bai2_tat_ca_tu' USING PigStorage(',');

top_5_words = LIMIT word_count_ordered 5;
rmf /home/duong/bai2_top5;
STORE top_5_words INTO '/home/duong/bai2_top5' USING PigStorage(',');