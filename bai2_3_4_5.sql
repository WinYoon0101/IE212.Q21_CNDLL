
-- 1. Khởi tạo
CREATE DATABASE IF NOT EXISTS hotel_lab2;
USE hotel_lab2;

-- Bảng raw data
CREATE EXTERNAL TABLE IF NOT EXISTS reviews_raw (id STRING, comment STRING, category STRING, aspect STRING, sentiment STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' LOCATION '/user/duong/LAB2/raw_data';

-- Bảng data sạch (lấy từ kết quả Pig Bài 1)
CREATE EXTERNAL TABLE IF NOT EXISTS cleaned_words (category STRING, aspect STRING, sentiment STRING, word STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/duong/LAB2/results/bai1_ketqua';


-- BÀI 2: Thống kê số bình luận theo Category và Aspect
INSERT OVERWRITE DIRECTORY '/user/duong/LAB2/results/bai2_category' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT category, COUNT(*) FROM reviews_raw GROUP BY category;

INSERT OVERWRITE DIRECTORY '/user/duong/LAB2/results/bai2_aspect' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT aspect, COUNT(*) FROM reviews_raw GROUP BY aspect;


-- BÀI 3: Khía cạnh tích cực/tiêu cực nhất
INSERT OVERWRITE DIRECTORY '/user/duong/LAB2/results/bai3_negative' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT aspect, COUNT(*) as neg FROM reviews_raw WHERE sentiment = 'negative' GROUP BY aspect ORDER BY neg DESC LIMIT 1;

INSERT OVERWRITE DIRECTORY '/user/duong/LAB2/results/bai3_positive' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT aspect, COUNT(*) as pos FROM reviews_raw WHERE sentiment = 'positive' GROUP BY aspect ORDER BY pos DESC LIMIT 1;


-- BÀI 4: Top 5 từ theo Category (Tích cực & Tiêu cực)
-- Tích cực
INSERT OVERWRITE DIRECTORY '/user/duong/LAB2/results/bai4_positive' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT category, word, freq FROM (SELECT category, word, COUNT(*) as freq, ROW_NUMBER() OVER(PARTITION BY category ORDER BY COUNT(*) DESC) as rank FROM cleaned_words WHERE sentiment = 'positive' GROUP BY category, word) r WHERE rank <= 5;

-- Tiêu cực
INSERT OVERWRITE DIRECTORY '/user/duong/LAB2/results/bai4_negative' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT category, word, freq FROM (SELECT category, word, COUNT(*) as freq, ROW_NUMBER() OVER(PARTITION BY category ORDER BY COUNT(*) DESC) as rank FROM cleaned_words WHERE sentiment = 'negative' GROUP BY category, word) r WHERE rank <= 5;


-- BÀI 5: 5 từ liên quan nhất theo từng phân loại
INSERT OVERWRITE DIRECTORY '/user/duong/LAB2/results/bai5_related' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT category, word, freq FROM (SELECT category, word, COUNT(*) as freq, ROW_NUMBER() OVER(PARTITION BY category ORDER BY COUNT(*) DESC) as rank FROM cleaned_words GROUP BY category, word) r WHERE rank <= 5;