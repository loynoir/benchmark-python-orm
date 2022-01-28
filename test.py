import contextlib
import sqlite3
import sys
import tempfile
import time

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import __version__, Column, Integer, String, create_engine, insert
from sqlalchemy.orm import Session

import peewee
from peewee import (
    SqliteDatabase,
    Model,
    IntegerField,
    TextField
)

Base = declarative_base()


class Customer(Base):
    __tablename__ = "customer"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))


@contextlib.contextmanager
def sqlalchemy_session(future):
    with tempfile.NamedTemporaryFile(suffix=".db") as handle:
        dbpath = handle.name
        engine = create_engine(f"sqlite:///{dbpath}", future=future, echo=False)
        session = Session(
            bind=engine, future=future, autoflush=False, expire_on_commit=False
        )
        Base.metadata.create_all(engine)
        yield session
        session.close()


def print_result(name, nrows, seconds):
    print(f"{name}:\n{' '*10}Total time for {nrows} records {seconds:.3f} secs")


def test_sqlalchemy_orm(n=100000, future=True):
    with sqlalchemy_session(future) as session:
        t0 = time.time()
        for i in range(n):
            customer = Customer()
            customer.name = "NAME " + str(i)
            session.add(customer)
            if i % 1000 == 0:
                session.flush()
        session.commit()
        print_result("SQLA ORM", n, time.time() - t0)


def test_sqlalchemy_orm_pk_given(n=100000, future=True):
    with sqlalchemy_session(future) as session:
        t0 = time.time()
        for i in range(n):
            customer = Customer(id=i + 1, name="NAME " + str(i))
            session.add(customer)
            if i % 1000 == 0:
                session.flush()
        session.commit()
        print_result("SQLA ORM pk given", n, time.time() - t0)


def test_sqlalchemy_orm_bulk_save_objects(n=100000, future=True, return_defaults=False):
    with sqlalchemy_session(future) as session:
        t0 = time.time()
        for chunk in range(0, n, 10000):
            session.bulk_save_objects(
                [
                    Customer(name="NAME " + str(i))
                    for i in range(chunk, min(chunk + 10000, n))
                ],
                return_defaults=return_defaults,
            )
        session.commit()
        print_result(
            f"SQLA ORM bulk_save_objects{', return_defaults' if return_defaults else ''}",
            n,
            time.time() - t0,
        )


def test_sqlalchemy_orm_bulk_insert(n=100000, future=True, return_defaults=False):
    with sqlalchemy_session(future) as session:
        t0 = time.time()
        for chunk in range(0, n, 10000):
            session.bulk_insert_mappings(
                Customer,
                [
                    dict(name="NAME " + str(i))
                    for i in range(chunk, min(chunk + 10000, n))
                ],
                return_defaults=return_defaults,
            )
        session.commit()
        print_result(
            f"SQLA ORM bulk_insert_mappings{', return_defaults' if return_defaults else ''}",
            n,
            time.time() - t0,
        )


def test_sqlalchemy_core(n=100000, future=True):
    with sqlalchemy_session(future) as session:
        with session.bind.begin() as conn:
            t0 = time.time()
            conn.execute(
                insert(Customer.__table__),
                [{"name": "NAME " + str(i)} for i in range(n)],
            )
            conn.commit()
            print_result("SQLA Core", n, time.time() - t0)


@contextlib.contextmanager
def sqlite3_conn():
    with tempfile.NamedTemporaryFile(suffix=".db") as handle:
        dbpath = handle.name
        conn = sqlite3.connect(dbpath)
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS customer")
        c.execute(
            "CREATE TABLE customer (id INTEGER NOT NULL, "
            "name VARCHAR(255), PRIMARY KEY(id))"
        )
        conn.commit()
        yield conn


def test_sqlite3(n=100000):
    with sqlite3_conn() as conn:
        c = conn.cursor()
        t0 = time.time()
        for i in range(n):
            row = ("NAME " + str(i),)
            c.execute("INSERT INTO customer (name) VALUES (?)", row)
        conn.commit()
        print_result("sqlite3", n, time.time() - t0)


def getPeeweeCustomer(db):
    class PeeweeCustomer(Model):
        id = IntegerField(primary_key=True)
        name = TextField()

        class Meta:
            database = db

    return PeeweeCustomer


def test_peewee_simple(n=100000):
    with tempfile.NamedTemporaryFile(suffix=".db") as handle:
        dbpath = handle.name
        db = SqliteDatabase(dbpath)
        PeeweeCustomer = getPeeweeCustomer(db)
        db.create_tables([PeeweeCustomer])

        t0 = time.time()
        for i in range(n):
            PeeweeCustomer.create(name="NAME " + str(i))
        print_result("peewee simple", n, time.time() - t0)


def test_peewee_atomic(n):
    with tempfile.NamedTemporaryFile(suffix=".db") as handle:
        dbpath = handle.name
        db = SqliteDatabase(dbpath)
        PeeweeCustomer = getPeeweeCustomer(db)
        db.create_tables([PeeweeCustomer])

        t0 = time.time()
        with db.atomic():
            for i in range(n):
                PeeweeCustomer.create(name="NAME " + str(i))
        print_result("peewee atomic", n, time.time() - t0)


if __name__ == "__main__":
    rows = 100000
    _future = True
    print(f"Python: {' '.join(sys.version.splitlines())}")
    print(f"sqlalchemy v{__version__} (future={_future})")
    print(f'peewee v{peewee.__version__}')
    # test_peewee_simple(rows)
    test_peewee_atomic(rows)
    test_sqlalchemy_orm(rows, _future)
    test_sqlalchemy_orm_pk_given(rows, _future)
    test_sqlalchemy_orm_bulk_save_objects(rows, _future)
    test_sqlalchemy_orm_bulk_save_objects(rows, _future, True)
    test_sqlalchemy_orm_bulk_insert(rows, _future)
    test_sqlalchemy_orm_bulk_insert(rows, _future, True)
    test_sqlalchemy_core(rows, _future)
    test_sqlite3(rows)
