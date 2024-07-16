import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Row
from sqlalchemy.schema import CreateTable
from sqlalchemy.types import *
from sqlalchemy import MetaData
from sqlalchemy.sql import insert as Insert
import datetime
import sys

engine = create_engine('mssql+pyodbc:///:memory:', echo = True)

custom_strings = {"AGE", "TEST"}
custom_floats = {"PRICE"}

def log(message):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S.%f')}] {message}")

def infer_column_type(tag, value):
    if tag in custom_strings:
        return String
    if tag in custom_floats:
        return Float
    try:
        int(value)
        return Integer
    except ValueError:
        return String
    
def parse_xml(file_path):
    log("Parsing XML file...")
    tree = ET.parse(file_path)
    root = tree.getroot()
    log("... parsed")
    return root

def create_sql_schema(xml_root):
    metadata = MetaData()
    tables = {}

    # ROWS – Determine all columns of the table
    identified_rows = {}
    for xml_elem in xml_root:
        for item in xml_elem:
            identified_rows[item.tag] = infer_column_type(item.tag, item.text)
    log(f'... identified {len(identified_rows)} different rows in the XML file')

    table_name = xml_root[0].tag
    log(f'... identified {table_name} as table name')

    columns = []
    for rowname, rowtype in identified_rows.items():
        column = Column(rowname, rowtype)
        columns.append(column)
    tables[table_name] = Table(table_name, metadata, *columns)
    return metadata, tables

def generate_sql_script(metadata, tables):
    log("Generate DDL schema...")
    sql_script = ""
    for table in tables.values():
        sql_script += str(CreateTable(table).compile(engine)) + ";\n\n"
    return sql_script

def generate_insert_script(tables, xml_root):
    log("Generate DCL inserts...")

    insert_script = ""
    counter=0
    for table in tables.values():
        for row in xml_root:
            values = {column.tag: column.text for column in row}
            counter += 1
            if counter % 50000 == 0:
                log(f"... processed {counter} rows")
            insert_stmt = Insert(table).values(values)
            compiled_stmt = insert_stmt.compile(compile_kwargs={"literal_binds": True})
            insert_script += str(compiled_stmt) + ";\n"
    log(f"... processed {counter} rows in total")
    
    return insert_script


if __name__ == "__main__":
    if sys.argv[1]:
        filename = sys.argv[1]
    else:
        filename = 'ecode1'
    
    log("Processing file {filename}")
    xml_file_path = f'xml/{filename}.xml'
    xml_root = parse_xml(xml_file_path)
    metadata, tables = create_sql_schema(xml_root)

    sql_script = generate_sql_script(metadata, tables)
    with open(f'sql/{filename}_schema.sql', 'w') as schema_file:
         schema_file.write(sql_script)

    insert_script = generate_insert_script(tables, xml_root)
    with open(f'sql/{filename}_insert.sql', 'w') as insert_file:
        insert_file.write(insert_script)
