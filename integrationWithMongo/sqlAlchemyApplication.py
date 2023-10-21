"""
    Challenge: integrating Python with SQLite and MongoDB:
    This code is a challenge proposed for DIO, the idea is to create
    a schema using databases of both types: SQL and NoSQL.
"""
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy import func
from sqlalchemy import Float

Base = declarative_base()


class ClientAccount(Base):
    """
        This class represents the table ClientAccount in SQLite
    """
    __tablename__ = "client"
    # Attributes
    ID = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(10))
    cpf = Column(String(11))
    address = Column(String(30))

    account = relationship(
        "Account", back_populates="client"
    )

    def __repr__(self):
        return f"Client: (ID={self.ID}, name={self.name}, cpf={self.cpf}, address={self.address})"


class Account(Base):
    """
        This class represents the table Account in SQLite
    """
    __tablename__ = "account"
    ID = Column(Integer, primary_key=True, autoincrement=True)
    type_account = Column(String(30), nullable=False)
    agency = Column(String(20))
    number_account = Column(String(12))
    cliente_ID = Column(Integer, ForeignKey("client.ID"), nullable=False)
    balance = Column(Float(5), nullable=False)

    client = relationship("ClientAccount", back_populates="account")

    def __repr__(self):
        return (f"Account(type_account={self.type_account}, agency={self.agency},"
                f" number_account={self.number_account}, balance={self.balance})")


# Connection with database
engine = create_engine("sqlite://")

# Creating the classes as tables on database
Base.metadata.create_all(engine)

# Inspect the database schema
inspector_engine = inspect(engine)

# Verifying if the table 'client' exists
print(inspector_engine.has_table("client"))

# Recovering all table names
print(inspector_engine.get_table_names())

# Recovering the default schema name
print(inspector_engine.default_schema_name)

for field in inspector_engine.get_columns("account"):
    print(field)

with Session(engine) as session:
    victor = ClientAccount(
        name='Victor',
        cpf='00800700622',
        address='Rio Largo',
    )

    hugo = ClientAccount(
        name='Hugo',
        cpf='12345678910',
        address='Maceió'
    )

    jose = ClientAccount(name='jose', cpf='09876543211',)

    # Sending to database (persistence data)
    session.add_all([victor, hugo, jose])

    session.commit()

stmt = select(ClientAccount).where(ClientAccount.name.in_(['Victor']))
for row_in in session.scalars(stmt):
    print(row_in)

with Session(engine) as session:
    victor_account = Account(
        type_account='checking account',
        agency='123',
        number_account='11111111111',
        balance=300,
        cliente_ID=1

    )

    hugo_account = Account(
        type_account='savings account',
        agency='321',
        number_account='22222222222',
        balance=300,
        cliente_ID=2
    )

    # Sending to database (persistence data)
    session.add_all([victor_account, hugo_account])

    session.commit()

print("Recovering information using LIKE")
stmt_address = select(Account).where(Account.agency.like('%2%'))
for row_like in session.scalars(stmt_address):
    print(row_like)

print("\nRecovering information by ordinated form")
stmt_order = select(ClientAccount).order_by(ClientAccount.name.desc())
for row_ordinated in session.scalars(stmt_order):
    print(row_ordinated)

print("\nRecovering data using INNER JOIN")
stmt_join = select(ClientAccount.cpf, Account.type_account).join_from(Account, ClientAccount)
for row_join in session.scalars(stmt_join):
    print(row_join)

connection = engine.connect()
results = connection.execute(stmt_join).fetchall()

print("\nExecuting a statement for a connection")
for result in results:
    print(result)

print("\nRecovering data using COUNT")
stmt_count = select(func.count('*')).select_from(ClientAccount)
for result in results:
    print(result)
