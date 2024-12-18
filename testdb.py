import time
import random
import csv
from sqlalchemy import create_engine, Index
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateIndex
from app import Author, Book

# Параметри підключення до бази даних
DATABASE_URL = "dataurl"

# Підключення до бази даних
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Додавання індексів до таблиць
def create_indexes():
    with engine.connect() as connection:
        index_statements = [
            CreateIndex(Index("idx_author_name", Author.__table__.c.name)),
            CreateIndex(Index("idx_book_title", Book.__table__.c.title)),
        ]
        for statement in index_statements:
            connection.execute(statement)
        print("Індекси створено.")

# Вимірювання часу
def measure_time(func, *args):
    start = time.time()
    result = func(*args)
    elapsed = round(time.time() - start, 4)
    return elapsed, result

# Очистка таблиць
def clear_tables(session):
    session.query(Book).delete()
    session.query(Author).delete()
    session.commit()

# Операції для тестування
def insert_rows(session, count):
    authors = [
        Author(name=f"Author {random.randint(1, 1000)}", books=[
            Book(title=f"Book {random.randint(1, 1000)}")
        ]) for _ in range(count)
    ]
    session.bulk_save_objects(authors)
    session.commit()

def select_rows(session, count):
    return session.query(Author).filter(Author.name.like("Author%")).limit(count).all()

def update_rows(session, count):
    ids = session.query(Author.id).limit(count).all()
    ids = [id[0] for id in ids]
    session.query(Author).filter(Author.id.in_(ids)).update(
        {Author.name: f"Updated {random.randint(1, 1000)}"},
        synchronize_session=False
    )
    session.commit()

def delete_rows(session, count):
    ids = session.query(Author.id).limit(count).all()
    ids = [id[0] for id in ids]
    session.query(Author).filter(Author.id.in_(ids)).delete(synchronize_session=False)
    session.commit()

# Основна функція
def test_operations_with_indexes():
    row_counts = [1000, 10000, 100000, 1000000]  # Обсяги даних
    operations = [
        ("INSERT", insert_rows),
        ("SELECT", select_rows),
        ("UPDATE", update_rows),
        ("DELETE", delete_rows),
    ]

    results = []  # Збираємо результати для таблиці

    create_indexes()  # Створення індексів перед тестуванням

    for operation_name, operation_func in operations:
        print(f"\nTesting {operation_name} operation with indexes:")

        for count in row_counts:
            with Session() as session:
                clear_tables(session)  
                
                if operation_name != "INSERT":
                    insert_rows(session, count)

                # Вимірювання часу операції
                elapsed_time, _ = measure_time(operation_func, session, count)

                # Збереження результатів
                results.append([operation_name, count, elapsed_time])
                print(f"{operation_name} {count} records: {elapsed_time} seconds")

    # Запис результатів у CSV
    with open("performance_results_with_indexes.csv", mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Operation", "Record Count", "Elapsed Time (s)"])
        writer.writerows(results)
    print("\nResults saved to 'performance_results_with_indexes.csv'")

if __name__ == "__main__":
    test_operations_with_indexes()