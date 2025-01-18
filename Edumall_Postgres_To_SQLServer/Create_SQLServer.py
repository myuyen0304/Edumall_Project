from sqlalchemy import create_engine, Column, Integer, NVARCHAR, Date, Numeric, ForeignKey , MetaData
from sqlalchemy.orm import declarative_base

# Create an in-memory SQLite database engine
engine = create_engine(r"mssql+pyodbc://DESKTOP-FG4LUUQ\SQLEXPRESS/edumall_db?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")

# Define Table Classes
Base = declarative_base()


class Author(Base):
    __tablename__ = 'Author'

    author_id = Column(Integer, primary_key=True, autoincrement= False)
    author_name = Column(NVARCHAR(255), unique=True, nullable=False)

class Topic(Base):
    __tablename__ = 'Topic'

    topic_id = Column(Integer, primary_key=True, autoincrement= False)
    topic_name = Column(NVARCHAR(255), unique=True, nullable=False)
    
class Course(Base):
    __tablename__ = 'Course'

    course_id = Column(Integer, primary_key=True, autoincrement= False)
    coursename = Column(NVARCHAR(255), nullable=False)
    describe = Column(NVARCHAR, nullable=True)  # NVARCHAR(MAX) mặc định trong SQLAlchemy
    newfee = Column(Integer)
    oldfee = Column(Integer)
    rating = Column(Numeric(2, 1))
    time = Column(Integer)
    full_date = Column(Date)  
    day = Column(Integer)     
    month = Column(Integer)   
    year = Column(Integer)    
    sections = Column(Integer)
    lectures = Column(Integer)
    what_you_will_learn = Column(NVARCHAR(3000))
    author_id = Column(Integer, ForeignKey('Author.author_id'))
    topic_id = Column(Integer, ForeignKey('Topic.topic_id'))

# Create the tables in the in-memory database
Base.metadata.create_all(engine)

# Print the names of all tables in the database
def print_all_tables(engine):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    
    tables = metadata.tables.keys()
    
    print("List of tables:")
    for table in tables:
        print(table)

# Print all tables in the in-memory database
print_all_tables(engine) 