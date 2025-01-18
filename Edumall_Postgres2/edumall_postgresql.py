from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, regexp_extract, col, to_date, when, lit, concat_ws, dayofmonth, month, year
from pyspark.sql.types import DecimalType


# Khởi tạo phiên làm việc Spark
spark = SparkSession.builder \
    .appName("edumall") \
    .config("spark.mongodb.input.uri", "mongodb://mymongodb:27017/edumall2.edumall_data2") \
    .config("spark.mongodb.output.uri", "mongodb://mymongodb:27017/edumall2.edumall_data2") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()
     
# Đọc dữ liệu từ MongoDB
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# Bước 1: Chỉ định danh sách cột cần làm sạch
columns_to_clean = ['Describe']  # Cột cần làm sạch

# Bước 2: Giữ lại chữ cái có dấu và loại bỏ ký tự đặc biệt và biểu tượng cảm xúc
for column in columns_to_clean:
    df = df.withColumn(column, regexp_replace(col(column), r"[^\w\sÀ-ỹ]", ""))  # Bao gồm Unicode cho chữ cái có dấu

# Bước 3: Thay thế "tháng" và "năm" bằng dấu gạch ngang trong cột 'Last_updated'
df = df.withColumn("Last_updated", regexp_replace("Last_updated", " tháng ", "-")) \
       .withColumn("Last_updated", regexp_replace("Last_updated", " năm ", "-"))

# Bước 4: Thay thế 'Bài học' và 'Chương' bằng chuỗi rỗng trong các cột tương ứng
df = df.withColumn('Lectures', regexp_replace('Lectures', 'Bài học', '')) \
       .withColumn('Sections', regexp_replace('Sections', 'Chương', ''))

# Bước 5: Chuyển đổi thời gian
df = df.withColumn(
    "Time",
    (regexp_extract(col("Time"), r'(\d+) giờ', 1).cast("int") * 60 +
     regexp_extract(col("Time"), r'(\d+) phút', 1).cast("int"))
)

# Bước 6: Kết hợp các giá trị trong cột 'What_you_will_learn'
df = df.withColumn("What_you_will_learn", concat_ws(", ", col("What_you_will_learn")))
df = df.withColumn("What_you_will_learn", when(col("What_you_will_learn") == "", lit(None)).otherwise(col("What_you_will_learn")))

# Bước 7: Chuyển đổi các cột sang kiểu dữ liệu thích hợp
df = df.withColumn("Newfee", col("Newfee").cast("integer")) \
       .withColumn("Oldfee", col("Oldfee").cast("integer")) \
       .withColumn("Lectures", col("Lectures").cast("integer")) \
       .withColumn("Sections", col("Sections").cast("integer")) \
       .withColumn("_id", regexp_replace(col("_id").cast("string"), "[{}]", ""))

# Bước 8: Chuyển đổi Rating thành kiểu số thập phân
df = df.withColumn("Rating", df["Rating"].cast(DecimalType(3, 2)))

# Bước 9: Chuyển đổi cột 'Last_updated' thành kiểu datetime và tách thành 3 cột ngày, tháng, năm
df = df.withColumn("Last_updated", to_date(df["Last_updated"], "d-M-yyyy")) \
       .withColumn("Last_day_updated", dayofmonth("Last_updated")) \
       .withColumn("Last_month_updated", month("Last_updated")) \
       .withColumn("Last_year_updated", year("Last_updated"))

# Bước 10: Thay thế giá trị NULL và rỗng bằng giá trị mặc định
df = df.fillna({"Coursename": "Unknown", "Newfee": 0, "Oldfee": 0, "Topic": "Unknown", "What_you_will_learn": "Updating Soon"})

df = df.withColumn('Author', when(col('Author').isNull(), 'Unknown')
                   .otherwise(col('Author')))
df = df.withColumn('Author', when(col('Author') == "", 'Unknown')
                   .otherwise(col('Author')))

df = df.withColumn('Describe', when(col('Describe').isNull(), 'Updating Soon')
                   .otherwise(col('Describe')))
df = df.withColumn('Describe', when(col('Describe') == "", 'Updating Soon')
                   .otherwise(col('Describe')))


#Bước 11: Xóa cột link và last_updated
df = df.drop("Link")
# df = df.drop("Last_updated")

# df_pandas = df.toPandas()
# df_pandas.to_excel('/Edumall_Postgres/edumall_clean.xlsx', index=False)
# print("Dữ liệu đã được ghi vào file Excel")

# Bước 12: Tạo DataFrame tác giả duy nhất
authors_df = df.select("Author").distinct().withColumnRenamed("Author", "author_name")

# Bước 13: Ghi các tác giả vào PostgreSQL
postgres_url = "jdbc:postgresql://mypostgres2:5432/edumall_db"
postgres_properties = {
    "user": "postgres",
    "password": "12345",
    "driver": "org.postgresql.Driver"
}

# Kiểm tra sự tồn tại của tác giả và ghi vào PostgreSQL
existing_authors_df = spark.read.jdbc(postgres_url, "Author", properties=postgres_properties)
new_authors_df = authors_df.join(existing_authors_df, "author_name", "left_anti")

if new_authors_df.count() > 0:
    new_authors_df.write.jdbc(
        url=postgres_url,
        table="Author",
        mode="append",
        properties=postgres_properties
    )

# Bước 15: Đọc lại bảng Author để lấy id
authors_with_id_df = spark.read.jdbc(postgres_url, "Author", properties=postgres_properties)

# Bước 16: Tạo DataFrame chủ đề duy nhất
topics_df = df.select("Topic").distinct().withColumnRenamed("Topic", "topic_name")

# Bước 17: Ghi các chủ đề vào PostgreSQL
existing_topics_df = spark.read.jdbc(postgres_url, "Topic", properties=postgres_properties)
new_topics_df = topics_df.join(existing_topics_df, "topic_name", "left_anti")

if new_topics_df.count() > 0:
    new_topics_df.write.jdbc(
        url=postgres_url,
        table="Topic",
        mode="append",
        properties=postgres_properties
    )

# Bước 18: Đọc lại bảng Topic để lấy id
topics_with_id_df = spark.read.jdbc(postgres_url, "Topic", properties=postgres_properties)

# Bước 19: Ánh xạ ID tác giả và chủ đề trở lại DataFrame gốc
# Tham gia theo author_name để lấy author_id
df_with_author_id = df.join(
    authors_with_id_df,
    df.Author == authors_with_id_df.author_name,
    "left"
).drop("author_name")

# Tham gia theo topic_name để lấy topic_id
df_with_topic_id = df_with_author_id.join(
    topics_with_id_df,
    df.Topic == topics_with_id_df.topic_name,
    "left"
).drop("topic_name")

# Bước 19: Tạo DataFrame khóa học
course_df = df_with_topic_id.select(
    "Coursename", "Describe", "Newfee", "Oldfee", "Rating", "Time",
    col("Last_updated").alias("full_date"),
    col("Last_day_updated").alias("day"),
    col("Last_month_updated").alias("month"),
    col("Last_year_updated").alias("year"),
    "Sections", "Lectures", "What_you_will_learn",
    authors_with_id_df.author_id.alias("author_id"),
    topics_with_id_df.topic_id.alias("topic_id")
)

# Bước 20: Ghi khóa học vào PostgreSQL
course_df.write.jdbc(
    url=postgres_url,
    table="Course",
    mode="append",
    properties=postgres_properties
)

print("Dữ liệu đã được ghi vào PostgreSQL.")
