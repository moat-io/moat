from database import Database
from database.src.database_seeder import DatabaseSeeder

db: Database = Database()
db.connect(echo_statements=True)
database_seeder: DatabaseSeeder = DatabaseSeeder(db=db)
database_seeder.seed()
