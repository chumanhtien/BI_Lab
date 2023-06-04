import argparse
from pyspark.sql import SparkSession

# Định nghĩa các đối số command line
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="input path")
parser.add_argument("--output", help="output path")
args = parser.parse_args()

# Lấy đường dẫn đến file input và file output từ các đối số command line
input_path = args.input
output_path = args.output

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Đọc dữ liệu từ file input
text_rdd = spark.sparkContext.textFile(input_path)

# Thực hiện word count
word_counts = text_rdd.flatMap(lambda line: line.split()) \
                      .map(lambda word: (word, 1)) \
                      .reduceByKey(lambda a, b: a + b)

# Lưu kết quả word count vào file output
word_counts.saveAsTextFile(output_path)
# In kết quả Word Count
for word, count in word_counts.collect():
    print(f"{word}: {count}")

# Dừng SparkSession
spark.stop()
