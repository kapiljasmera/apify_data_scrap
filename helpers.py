from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import json
from dotenv import load_dotenv
from os import getenv

load_dotenv('.env')

db_user = getenv('db_user')
db_password = getenv('db_password')
db_host = getenv('db_host')
db_database = getenv('db_database')

# Create a database connection
db_connection_str = f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_database}'
db_connection = create_engine(db_connection_str)

Session = sessionmaker(db_connection)
sessionDB= Session()


def convert_columns_to_json(column_value):
    return json.dumps(column_value) if isinstance(column_value, (list, dict)) else column_value



def data_to_sql(dataFrame, db_table):
    # Insert the DataFrame into the MySQL table
    dataFrame.to_sql(db_table, con=db_connection, if_exists='replace', index=False)
    db_connection.dispose()
    print(f"Data loaded into MySQL successfully!")
    return

def get_url_from_sql(db_table):

    execute_query = text(f"SELECT webUrl FROM {db_table};")

    url = sessionDB.execute(execute_query)
    return url.fetchall()

def create_hotel_dim_table_trip(trip_dim, db_data_table):
    sessionDB.execute(text(f"""INSERT INTO {trip_dim} (created, updated, is_active, is_deleted, id, webUrl, name, localName, email, type, rankingPosition, description, priceLevel, category, address, addressObj, website, phone, latitude, longitude, rating, ratingHistogram, numberOfReviews, photoCount, image, rawRanking, priceRange, reviewTags, rankingString, subcategories) 
                SELECT now(), now(), 1, 0, tsh.id, tsh.webUrl, tsh.name, tsh.localName, tsh.email, tsh.type, tsh.rankingPosition, tsh.description, tsh.priceLevel, tsh.category, tsh.address, tsh.addressObj, tsh.website, tsh.phone, tsh.latitude, tsh.longitude, tsh.rating, tsh.ratingHistogram, tsh.numberOfReviews, tsh.photoCount, tsh.image, tsh.rawRanking, tsh.priceRange, tsh.reviewTags, tsh.rankingString, tsh.subcategories
                FROM {db_data_table} tsh
                LEFT JOIN {trip_dim} tdh
                ON tsh.id = tdh.id 
                WHERE tdh.id IS NULL;    
                    """))
    
    sessionDB.execute(text(f"""INSERT INTO {trip_dim} (created, updated, is_active, is_deleted, id, webUrl, name, localName, email, type, rankingPosition, description, priceLevel, category, address, addressObj, website, phone, latitude, longitude, rating, ratingHistogram, numberOfReviews, photoCount, image, rawRanking, priceRange, reviewTags, rankingString, subcategories) 
                SELECT now(), now(), 1, 0, tsh.id, tsh.webUrl, tsh.name, tsh.localName, tsh.email, tsh.type, tsh.rankingPosition, tsh.description, tsh.priceLevel, tsh.category, tsh.address, tsh.addressObj, tsh.website, tsh.phone, tsh.latitude, tsh.longitude, tsh.rating, tsh.ratingHistogram, tsh.numberOfReviews, tsh.photoCount, tsh.image, tsh.rawRanking, tsh.priceRange, tsh.reviewTags, tsh.rankingString, tsh.subcategories
                FROM {db_data_table} tsh
                LEFT JOIN {trip_dim} tdh
                ON tdh.id = tsh.id AND tsh.webUrl = tdh.webUrl
                WHERE tdh.id = tsh.id AND 
                (tsh.webUrl<>tdh.webUrl OR tsh.name<>tdh.name OR tsh.localName<>tdh.localName OR tsh.email<>tdh.email OR tsh.rankingPosition<>tdh.rankingPosition OR tsh.description<>tdh.description OR tsh.category<>tdh.category OR tsh.address<>tdh.address OR tsh.addressObj<>tdh.addressObj OR tsh.website<>tdh.website OR tsh.phone<>tdh.phone OR tsh.latitude<>tdh.latitude OR tsh.longitude<>tdh.longitude OR tsh.rating<>tdh.rating OR tsh.ratingHistogram<>tdh.ratingHistogram OR tsh.numberOfReviews<>tdh.numberOfReviews OR tsh.rawRanking<>tdh.rawRanking OR tsh.priceRange<>tdh.priceRange OR tsh.rankingString<>tdh.rankingString)
                AND tdh.updated >= (SELECT MAX(created) FROM {trip_dim} WHERE tdh.id = {trip_dim}.id) and is_active = 1;
                    """))
    
    sessionDB.execute(text(f"""UPDATE {trip_dim} tdh
                JOIN {db_data_table} tsh ON tsh.id = tdh.id
                SET is_active = 0, updated = now()
                WHERE (tsh.webUrl<>tdh.webUrl OR tsh.name<>tdh.name OR tsh.localName<>tdh.localName OR tsh.email<>tdh.email OR tsh.rankingPosition<>tdh.rankingPosition OR tsh.description<>tdh.description OR tsh.category<>tdh.category OR tsh.address<>tdh.address OR tsh.addressObj<>tdh.addressObj OR tsh.website<>tdh.website OR tsh.phone<>tdh.phone OR tsh.latitude<>tdh.latitude OR tsh.longitude<>tdh.longitude OR tsh.rating<>tdh.rating OR tsh.ratingHistogram<>tdh.ratingHistogram OR tsh.numberOfReviews<>tdh.numberOfReviews OR tsh.rawRanking<>tdh.rawRanking OR tsh.priceRange<>tdh.priceRange OR tsh.rankingString<>tdh.rankingString)
                AND is_active = 1;
                    """))
    
    sessionDB.execute(text(f"""UPDATE {trip_dim}
                SET is_active = 0, is_deleted = 1, updated = now()
                WHERE NOT EXISTS (SELECT 1 FROM {db_data_table} WHERE {db_data_table}.id = {trip_dim}.id);
                    """))

    sessionDB.commit()
    sessionDB.close()
    print('DONE')

def create_restaurant_dim_table_trip(trip_dim, db_data_table):
    sessionDB.execute(text(f"""INSERT INTO {trip_dim} (created, updated, is_active, is_deleted, id, webUrl, name, localName, email, menuWebUrl, type, rankingPosition, description, priceLevel, category, address, addressObj, website, phone, latitude, longitude, rating, ratingHistogram, numberOfReviews, photoCount, image, rawRanking, priceRange, reviewTags, isLongClosed, cuisines, mealTypes, dishes, features, dietaryRestrictions, rankingString, subcategories) 
                SELECT now(), now(), 1, 0, tsr.id, tsr.webUrl, tsr.name, tsr.localName, tsr.email, tsr.menuWebUrl, tsr.type, tsr.rankingPosition, tsr.description, tsr.priceLevel, tsr.category, tsr.address, tsr.addressObj, tsr.website, tsr.phone, tsr.latitude, tsr.longitude, tsr.rating, tsr.ratingHistogram, tsr.numberOfReviews, tsr.photoCount, tsr.image, tsr.rawRanking, tsr.priceRange, tsr.reviewTags, tsr.isLongClosed, tsr.cuisines, tsr.mealTypes, tsr.dishes, tsr.features, tsr.dietaryRestrictions, tsr.rankingString, tsr.subcategories
                FROM {db_data_table} tsr
                LEFT JOIN {trip_dim} tdr
                ON tdr.id = tsr.id
                WHERE tdr.id IS NULL; 
                   """))
    
    sessionDB.execute(text(f"""INSERT INTO {trip_dim} (created, updated, is_active, is_deleted, id, webUrl, name, localName, email, menuWebUrl, type, rankingPosition, description, priceLevel, category, address, addressObj, website, phone, latitude, longitude, rating, ratingHistogram, numberOfReviews, photoCount, image, rawRanking, priceRange, reviewTags, isLongClosed, cuisines, mealTypes, dishes, features, dietaryRestrictions, rankingString, subcategories) 
                SELECT now(), now(), 1, 0, tsr.id, tsr.webUrl, tsr.name, tsr.localName, tsr.email, tsr.menuWebUrl, tsr.type, tsr.rankingPosition, tsr.description, tsr.priceLevel, tsr.category, tsr.address, tsr.addressObj, tsr.website, tsr.phone, tsr.latitude, tsr.longitude, tsr.rating, tsr.ratingHistogram, tsr.numberOfReviews, tsr.photoCount, tsr.image, tsr.rawRanking, tsr.priceRange, tsr.reviewTags, tsr.isLongClosed, tsr.cuisines, tsr.mealTypes, tsr.dishes, tsr.features, tsr.dietaryRestrictions, tsr.rankingString, tsr.subcategories
                FROM {db_data_table} tsr
                LEFT JOIN {trip_dim} tdr
                ON tdr.id = tsr.id
                WHERE tdr.id = tsr.id AND
                (tsr.webUrl<>tdr.webUrl OR tsr.name<>tdr.name OR tsr.localName<>tdr.localName OR tsr.email<>tdr.email OR tsr.menuWebUrl<>tdr.menuWebUrl OR tsr.rankingPosition<>tdr.rankingPosition OR tsr.description<>tdr.description OR tsr.category<>tdr.category OR tsr.address<>tdr.address OR tsr.addressObj<>tdr.addressObj OR tsr.website<>tdr.website OR tsr.phone<>tdr.phone OR tsr.latitude<>tdr.latitude OR tsr.longitude<>tdr.longitude OR tsr.rating<>tdr.rating OR tsr.ratingHistogram<>tdr.ratingHistogram OR tsr.numberOfReviews<>tdr.numberOfReviews OR tsr.rawRanking<>tdr.rawRanking OR tsr.priceRange<>tdr.priceRange OR tsr.rankingString<>tdr.rankingString OR tsr.isLongClosed<>tdr.isLongClosed OR tsr.cuisines<>tdr.cuisines OR tsr.mealTypes<>tdr.mealTypes OR tsr.dishes<>tdr.dishes OR tsr.features<>tdr.features OR tsr.dietaryRestrictions<>tdr.dietaryRestrictions OR tsr.rankingString<>tdr.rankingString OR tsr.subcategories<>tdr.subcategories)
                AND tdr.updated >= (SELECT MAX(created) FROM {trip_dim} WHERE tdr.id = restaurant_dim.id) and is_active = 1;
                    """))
    
    sessionDB.execute(text(f"""UPDATE {trip_dim} tdr
                JOIN {db_data_table} tsr ON tsr.id = tdr.id
                SET is_active = 0, updated = now()
                WHERE (tsr.webUrl<>tdr.webUrl OR tsr.name<>tdr.name OR tsr.localName<>tdr.localName OR tsr.email<>tdr.email OR tsr.menuWebUrl<>tdr.menuWebUrl OR tsr.rankingPosition<>tdr.rankingPosition OR tsr.description<>tdr.description OR tsr.category<>tdr.category OR tsr.address<>tdr.address OR tsr.addressObj<>tdr.addressObj OR tsr.website<>tdr.website OR tsr.phone<>tdr.phone OR tsr.latitude<>tdr.latitude OR tsr.longitude<>tdr.longitude OR tsr.rating<>tdr.rating OR tsr.ratingHistogram<>tdr.ratingHistogram OR tsr.numberOfReviews<>tdr.numberOfReviews OR tsr.rawRanking<>tdr.rawRanking OR tsr.priceRange<>tdr.priceRange OR tsr.rankingString<>tdr.rankingString OR tsr.isLongClosed<>tdr.isLongClosed OR tsr.cuisines<>tdr.cuisines OR tsr.mealTypes<>tdr.mealTypes OR tsr.dishes<>tdr.dishes OR tsr.features<>tdr.features OR tsr.dietaryRestrictions<>tdr.dietaryRestrictions OR tsr.rankingString<>tdr.rankingString OR tsr.subcategories<>tdr.subcategories)
                AND is_active = 1;
                    """))
    sessionDB.execute(text(f"""UPDATE {trip_dim}
                SET is_active = 0, is_deleted = 1, updated = now()
                WHERE NOT EXISTS (SELECT 1 FROM {db_data_table} WHERE {db_data_table}.id = {trip_dim}.id);
                    """))
    

    sessionDB.commit()
    sessionDB.close()
    print('DONE')

