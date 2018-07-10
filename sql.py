import os
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import select
from mysql.connector import IntegrityError
engine = create_engine('mysql+mysqlconnector://root@localhost/db_name')
connection = engine.connect()

def main():
    #create_tables()
    #import_default_data()
    sql_stm()
    sqlalchemy_stm()
    connection.close()

def create_tables():
    """ Create Tables User and Group
    """
    metadata = MetaData()

    group = Table('group', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(16),unique= True, nullable=False),
    )

    user = Table('user', metadata,
    Column('user_id', Integer, primary_key=True),
    Column('user_name', String(16), nullable=False),
    Column('email_address', String(60)),
    Column('password', String(20), nullable=False),
    Column('group_id', Integer, ForeignKey("group.id"))
    )
    metadata.create_all(engine)

def import_default_data():
    group = reglection_table_group()
    groups_names =['manager','administrator','authenticated']
    for name in groups_names:
        ins = group.insert().values(name=name)
        try:
          result = connection.execute(ins)
        except :
            print('Ops! Error')
    user = reglection_table_user()
    ins = user.insert().values(user_name="Jhon W.", email_address="jhw@demo.com",password="d@2102",group_id=16)
    result = connection.execute(ins)

def reglection_table_user():
    """Reflection reads database and builds  SQLalchemy Tableobjects
    """
    metada = MetaData()
    user = Table('user',metada,autoload = True,autoload_with=engine)
    return user;

def reglection_table_group():
    """Reflection reads database and builds  SQLalchemy Tableobjects
    """
    metada = MetaData()
    group = Table('group',metada,autoload = True,autoload_with=engine)
    return group;

def sql_stm():
    """Basic sql Queries
    """
    stm = "SELECT * FROM `user` JOIN `group` ON group.id = user.group_id"
    result_proxy = connection.execute(stm)
    rows = result_proxy.fetchall()
    for row in rows:
        print(row)

def sqlalchemy_stm():
    user = reglection_table_user();
    stm = select([user])
    rows = connection.execute(stm).fetchall()
    for row in rows:
        print(row)

if __name__ == "__main__":
    main()
