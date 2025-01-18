import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import sqlalchemy

# Bước 1: Kết nối với PostgreSQL
def connect_postgres():
    try:
        pg_conn = psycopg2.connect(
            host="127.0.0.1",  # Địa chỉ PostgreSQL container
            port="5432",       # Cổng PostgreSQL container
            database="edumall_db",
            user="postgres",
            password="12345"
        )
        print("Kết nối với PostgreSQL thành công.")
        return pg_conn
    except Exception as e:
        print(f"Không thể kết nối tới PostgreSQL: {e}")
        return None

# Bước 2: Trích xuất dữ liệu từ PostgreSQL
def fetch_data_from_postgres(query, conn):
    try:
        df = pd.read_sql(query, conn)
        print(f"Dữ liệu được trích xuất thành công từ truy vấn: {query[:50]}...")
        return df
    except Exception as e:
        print(f"Lỗi khi trích xuất dữ liệu từ PostgreSQL: {e}")
        return None

# Bước 3: Kết nối với Microsoft SQL Server
def connect_mssql():
    try:
        # Kết nối với SQL Server bằng Windows Authentication
        mssql_engine = create_engine(r"mssql+pyodbc://DESKTOP-FG4LUUQ\SQLEXPRESS/edumall_db?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")
        print("Kết nối với SQL Server thành công.")
        return mssql_engine
    except Exception as e:
        print(f"Không thể kết nối tới SQL Server: {e}")
        return None


# Bước 5: Tải dữ liệu vào Microsoft SQL Server
def load_data_to_mssql(df, table_name, engine):
    try:
        # Chuyển các cột kiểu object thành kiểu string (tương thích với NVARCHAR)
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype('string')

        # Điều chỉnh kiểu dữ liệu cho các cột của từng bảng
        dtype_mapping = {}

        if table_name == 'Author':
            dtype_mapping = {
                'author_name': sqlalchemy.types.NVARCHAR(length=255)
            }
        elif table_name == 'Topic':
            dtype_mapping = {
                'topic_name': sqlalchemy.types.NVARCHAR(length=255)
            }
        elif table_name == 'Course':
            dtype_mapping = {
                'coursename': sqlalchemy.types.NVARCHAR(length=255),
                'describe': sqlalchemy.types.NVARCHAR(),
                'what_you_will_learn': sqlalchemy.types.NVARCHAR(length=3000)
            }

        # Tải dữ liệu vào bảng SQL Server
        df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_mapping)
        print(f"Dữ liệu đã được tải thành công vào bảng {table_name}.")
    except Exception as e:
        print(f"Lỗi khi tải dữ liệu vào bảng {table_name}: {e}")

# Ngắt kết nối SQL Server
def disconnect_mssql(engine):
    try:
        engine.dispose()
        print("Kết nối SQL Server đã được đóng.")
    except Exception as e:
        print(f"Lỗi khi ngắt kết nối SQL Server: {e}")

# Hàm chính để điều phối quá trình ETL
def main():
    # Bước 1: Kết nối PostgreSQL
    pg_conn = connect_postgres()
    if pg_conn is None:
        return

    # Bước 2: Lấy dữ liệu từ PostgreSQL
    author_df = fetch_data_from_postgres("SELECT * FROM Author;", pg_conn)
    topic_df = fetch_data_from_postgres("SELECT * FROM Topic;", pg_conn)
    course_df = fetch_data_from_postgres("SELECT * FROM Course;", pg_conn)

    # Đóng kết nối PostgreSQL
    pg_conn.close()

    # Kiểm tra xem có DataFrame nào rỗng hoặc None không
    if any(df is None or df.empty for df in [author_df, topic_df, course_df]):
        print("Lỗi: Một hoặc nhiều DataFrame là None hoặc rỗng. Dừng quá trình.")
        return

    # Bước 3: Kết nối SQL Server
    mssql_engine = connect_mssql()
    if mssql_engine is None:
        print('Lỗi kết nối SQL Server')
        return

    # Bước 4: Tải dữ liệu vào SQL Server
    load_data_to_mssql(author_df, 'Author', mssql_engine)
    load_data_to_mssql(topic_df, 'Topic', mssql_engine)
    load_data_to_mssql(course_df, 'Course', mssql_engine)
    
    # Ngắt kết nối SQL Server sau khi tải dữ liệu
    # disconnect_mssql(mssql_engine)

if __name__ == "__main__":
    main()