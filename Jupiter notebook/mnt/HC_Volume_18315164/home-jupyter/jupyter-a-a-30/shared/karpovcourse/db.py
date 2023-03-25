from sqlalchemy import create_engine
POSTGRES_ADDRESS = 'lab.karpov.courses'
POSTGRES_PORT = 5432
POSTGRES_USERNAME = 'student'
POSTGRES_PASSWORD = '123'
POSTGRES_DBNAME = 'test'

postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'
                .format(username=POSTGRES_USERNAME,
                        password=POSTGRES_PASSWORD,
                        ipaddress=POSTGRES_ADDRESS,
                        port=POSTGRES_PORT,
                        dbname=POSTGRES_DBNAME))
cnx = create_engine(postgres_str, connect_args={'sslmode':'require'})
