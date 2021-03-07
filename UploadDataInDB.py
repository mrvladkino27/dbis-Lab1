import psycopg2
import csv
import glob
import ConnectDB
from Settings import profile_time
from typing import Tuple
from decimal import Decimal


table_columns_types = {
    "OUTID": "varchar(100)",
    "Birth": "smallint",
    "GenderTYPENAME": "Gender",
    "REGNAME": "TEXT",
    "AREANAME": "TEXT",
    "TERNAME": "TEXT",
    "REGTYPENAME": "TEXT",
    "TerTypeName": "ter",
    "ClassProfileNAME": "TEXT",
    "ClassLangName": "TEXT",
    "EONAME": "TEXT",
    "EOTYPENAME": "TEXT",
    "EORegName": "TEXT",
    "EOAreaName": "TEXT",
    "EOTerName": "TEXT",
    "EOParent": "TEXT",
    "UkrTest": "TEXT",
    "UkrTestStatus": "TEXT",
    "UkrBall100": "numeric",
    "UkrBall12": "numeric",
    "UkrBall": "numeric",
    "UkrAdaptScale": "numeric",
    "UkrPTName": "TEXT",
    "UkrPTRegName": "TEXT",
    "UkrPTAreaName": "TEXT",
    "UkrPTTerName": "TEXT",
    "histTest": "TEXT",
    "HistLang": "TEXT",
    "histTestStatus": "TEXT",
    "histBall100": "numeric",
    "histBall12": "numeric",
    "histBall": "numeric",
    "histPTName": "TEXT",
    "histPTRegName": "TEXT",
    "histPTAreaName": "TEXT",
    "histPTTerName": "TEXT",
    "mathTest": "TEXT",
    "mathLang": "TEXT",
    "mathTestStatus": "TEXT",
    "mathBall100": "numeric",
    "mathBall12": "numeric",
    "mathBall": "numeric",
    "mathPTName": "TEXT",
    "mathPTRegName": "TEXT",
    "mathPTAreaName": "TEXT",
    "mathPTTerName": "TEXT",
    "physTest": "TEXT",
    "physLang": "TEXT",
    "physTestStatus": "TEXT",
    "physBall100": "numeric",
    "physBall12": "numeric",
    "physBall": "numeric",
    "physPTName": "TEXT",
    "physPTRegName": "TEXT",
    "physPTAreaName": "TEXT",
    "physPTTerName": "TEXT",
    "chemTest": "TEXT",
    "chemLang": "TEXT",
    "chemTestStatus": "TEXT",
    "chemBall100": "numeric",
    "chemBall12": "numeric",
    "chemBall": "numeric",
    "chemPTName": "TEXT",
    "chemPTRegName": "TEXT",
    "chemPTAreaName": "TEXT",
    "chemPTTerName": "TEXT",
    "bioTest": "TEXT",
    "bioLang": "TEXT",
    "bioTestStatus": "TEXT",
    "bioBall100": "numeric",
    "bioBall12": "numeric",
    "bioBall": "numeric",
    "bioPTName": "TEXT",
    "bioPTRegName": "TEXT",
    "bioPTAreaName": "TEXT",
    "bioPTTerName": "TEXT",
    "geoTest": "TEXT",
    "geoLang": "TEXT",
    "geoTestStatus": "TEXT",
    "geoBall100": "numeric",
    "geoBall12": "numeric",
    "geoBall": "numeric",
    "geoPTName": "TEXT",
    "geoPTRegName": "TEXT",
    "geoPTAreaName": "TEXT",
    "geoPTTerName": "TEXT",
    "engTest": "TEXT",
    "engTestStatus": "TEXT",
    "engBall100": "numeric",
    "engBall12": "numeric",
    "engDPALevel": "TEXT",
    "engBall": "numeric",
    "engPTName": "TEXT",
    "engPTRegName": "TEXT",
    "engPTAreaName": "TEXT",
    "engPTTerName": "TEXT",
    "fraTest": "TEXT",
    "fraTestStatus": "TEXT",
    "fraBall100": "numeric",
    "fraBall12": "numeric",
    "fraDPALevel": "TEXT",
    "fraBall": "numeric",
    "fraPTName": "TEXT",
    "fraPTRegName": "TEXT",
    "fraPTAreaName": "TEXT",
    "fraPTTerName": "TEXT",
    "deuTest": "TEXT",
    "deuTestStatus": "TEXT",
    "deuBall100": "numeric",
    "deuBall12": "numeric",
    "deuDPALevel": "TEXT",
    "deuBall": "numeric",
    "deuPTName": "TEXT",
    "deuPTRegName": "TEXT",
    "deuPTAreaName": "TEXT",
    "deuPTTerName": "TEXT",
    "spaTest": "TEXT",
    "spaTestStatus": "TEXT",
    "spaBall100": "numeric",
    "spaBall12": "numeric",
    "spaDPALevel": "TEXT",
    "spaBall": "numeric",
    "spaPTName": "TEXT",
    "spaPTRegName": "TEXT",
    "spaPTAreaName": "TEXT",
    "spaPTTerName": "TEXT",
    "year": "smallint",
}


NamePath_files = 'SourceData/*.csv'
FileNamePath= dict((path[-12:-8], path) for path in glob.glob(
    NamePath_files
))

@profile_time            
def create_table(drop_if_exists=True) -> None:
    """
    Create table with prepared structure
    """
    connection = ConnectDB.connect()
    additional_types_query = """DO
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'Gender') THEN
                CREATE TYPE Gender AS ENUM ('female', 'man');
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ter') THEN
                CREATE TYPE ter AS ENUM ('city', 'village');
            END IF;
        END;"""

    ZnoData_drop_query = "DROP TABLE IF EXISTS ZnoData;"

    ZnoData_create_query = 'CREATE TABLE IF NOT EXISTS ZnoData ('
    fields = ''
    for col, t in table_columns_types.items():
        if fields == '':
            fields +=  (f'{col} {t}') + ' PRIMARY KEY, '
        else:
            fields +=  (f'{col} {t}')+ ', '
    ZnoData_create_query += fields[:-2] + ');'
    
    with connection.cursor() as cursor:
        # cursor.execute(additional_types_query)
        if drop_if_exists:
            cursor.execute(ZnoData_drop_query)
            print('Table was droped.')
        cursor.execute(ZnoData_create_query)
        print('Table was created.')
    ConnectDB.disconnect(connection)
        
@profile_time
def get_max_eng_2019_2020(connection) -> Tuple[Tuple[str, Decimal, Decimal], Tuple[psycopg2.extensions.Column]]:
    query = """
    select res2019.regname  as "District",
           res2019.eng_max as "English 2019 max",
           res2020.eng_max as "English 2020 max"
    from (select regname, max(engball100) eng_max
           from ZnoData
           where ZnoData.engteststatus = 'Зараховано'
             and ZnoData.year = 2019
           group by ZnoData.regname) as res2019
             join
         (select regname, max(engball100) as eng_max
          from ZnoData
          where ZnoData.engteststatus = 'Зараховано'
            and ZnoData.year = 2020
          group by ZnoData.regname) as res2020
         on res2019.regname = res2020.regname
    order by "District";
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        return tuple(col.name for col in cursor.description), tuple(cursor.fetchall())
    
    
@profile_time            
def from_csv_to_database():
    """
    Load csv file to database. Add `year` column
    """
    for year, path in FileNamePath.items():
        # load csv files
        with open(path, encoding='cp1251') as dataset:
            print(f"Download {year} data")
            get_curr_data(dataset, year)
            
def get_curr_data(file, year: int):
    connection = ConnectDB.connect()
    data = csv.reader(file, delimiter=';', quotechar='"')
    header = list(map(str.lower, next(data)))
    header.append('year')
    header[header.index('sextypename')] = 'gendertypename'
    num_of_rows = 0
    
    for row in data:
        num_of_rows += 1
        insert_row = tuple(row)
        insert_row = (*insert_row, year)
        insert_row_in_DB(insert_row, tuple(header), connection)
        print(num_of_rows, ' <=> ', year)

    print(f'Inserted {num_of_rows} rows from {year} into table ZnoData')

def insert_row_in_DB(insert_row, header, conn):
    insert_query = 'INSERT INTO ZnoData ('
    for column_name in header:
        insert_query += column_name + ', '
    insert_query = insert_query[:-2]
    insert_query += ') VALUES(' 
    for value in insert_row:
        if value != clean_csv_value(value):
            insert_query += clean_csv_value(value) + ', '
        else:
            if value == 'null':
                insert_query += value + ', '
            else:
                insert_query += '\'' + clean_csv_value(value).replace('\'','*') + '\', '
    insert_query = insert_query[:-2]
    insert_query += ');'
    
    try:
        with conn.cursor() as cur:
            cur.execute(insert_query)
    except psycopg2.errors.UniqueViolation:
        pass
    except psycopg2.errors.InFailedSqlTransaction:
        pass
    except Exception as e:
        print(e)

def clean_csv_value(value):
    if value == 'null':
        return value
    try:
        res = float(value.replace(',', '.'))
        return str(res)
    except:
        return value
    
def to_csv(header, rows):
    """
    Save csv file with given header and rows into output folder
    """
    with open('result.csv', 'w') as result:
        result_writer = csv.writer(result, delimiter=';')
        result_writer.writerow(header)
        result_writer.writerows(rows)



if __name__ == '__main__':
    create_table(drop_if_exists=True)
    from_csv_to_database()
    conn = ConnectDB.connect()
    to_csv(*get_max_eng_2019_2020(conn))
    ConnectDB.disconnect(conn)