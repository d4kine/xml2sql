import csv
import datetime
import os
import sys
import xml.etree.ElementTree as ET
from argparse import ArgumentParser
import pathlib

from sqlalchemy import MetaData
from sqlalchemy import create_engine, Table, Column
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import insert as Insert
from sqlalchemy.types import *

# CONFIGURATION
engine = create_engine('mssql+pyodbc:///:memory:', echo=True)
custom_strings = {"AGE", "TEST"}
custom_floats = {"PRICE"}

# INTERNAL
all_rows = set()
xsd_type_mapping = {
    'xsd:byte': Integer,
    'xsd:short': Integer,
    'xsd:int': Integer,
    'xsd:long': BigInteger,
    'xsd:string': String,
    'xsd:decimal': Numeric,
    'xsd:float': Float,
    'xsd:double': Float,
}


def log(message):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S.%f')}] {message}")


def parse_xsd_location(xml_root):
    schema_location = xml_root.attrib['{http://www.w3.org/2001/XMLSchema-instance}noNamespaceSchemaLocation']
    return schema_location


def xsd_type_to_sql_type(xsd_type):
    return xsd_type_mapping.get(xsd_type, String)


def parse_xsd_schema(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    metadata = MetaData()
    tables = {}

    for element in root.findall('.//xsd:element', namespaces={'xsd': 'http://www.w3.org/2001/XMLSchema'}):
        complex_type = element.find('xsd:complexType/xsd:sequence', namespaces={'xsd': 'http://www.w3.org/2001/XMLSchema'})
        if complex_type is not None:

            for subelement in complex_type.findall('xsd:element', namespaces={'xsd': 'http://www.w3.org/2001/XMLSchema'}):

                if subelement is not None:
                    subelementname = subelement.get('name')
                    columns = []
                    for subsubelement in subelement.find('xsd:complexType/xsd:sequence', namespaces={'xsd': 'http://www.w3.org/2001/XMLSchema'}):
                        col_name = subsubelement.get('name')
                        if col_name is None:  # TODO this is hacky but if the name if not available, it must be a sub-sequence
                            continue
                        col_type = xsd_type_to_sql_type(subsubelement.get('type'))
                        col = Column(col_name, col_type)
                        columns.append(col)
                        all_rows.add(col_name)

                tables[subelementname] = Table(subelementname, metadata, *columns)
            break

    return metadata, tables


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
    counter = 0
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


def processXml():
    print("Processing XSD file...")


if __name__ == "__main__":
    # READ CLI ARGUMENTS
    parser = ArgumentParser()
    # Read arguments
    #if __debug__:
    #   sys.argv = ["main.py", "input/ecode1.xml"]

    passed_file = sys.argv[1]
    file = pathlib.Path(passed_file)
    file_ext = os.path.splitext(file)[1]
    full_path = file.parent.resolve()

    if file.exists() == False or file_ext != ".xml":
        print(f"File {file} does not exist or is not an XML file.")
        sys.exit(1)

    filename_plain = file.stem
    filename = os.path.basename(file)
    log(f"Processing file {full_path}/{file.name}")

    # PARSE XML GLOBALLY
    xml_root = parse_xml(file)

    # PARSE XSD LOCATION
    xsd_file = parse_xsd_location(xml_root)
    xsd_path = os.path.join(full_path, xsd_file)

    # GENERATE SCHEMA FROM XSD OR FROM XML
    if (os.path.exists(xsd_path)):
        log(f'XSD file found: {xsd_path}')
        metadata, tables = parse_xsd_schema(xsd_path)
        sql_script = generate_sql_script(metadata, tables)
        with open(f'output/{filename_plain}_schema.sql', 'w') as schema_file:
            schema_file.write(sql_script)
        xsdProcessed = True
    else:
        log(f'Referenced XSD file does not exist ({xsd_file}), determine schema by XML tags...')
        metadata, tables = create_sql_schema(xml_root)
        sql_script = generate_sql_script(metadata, tables)
        with open(f'output/{filename_plain}_schema.sql', 'w') as schema_file:
            schema_file.write(sql_script)

    # GENERATE DCL
    log("Generate DCL inserts...")
    # values = {column.tag: column.text for column in col}
    # counter += 1
    # if counter % 50000 == 0:
    #    log(f"... processed {counter} rows")
    # insert_stmt = Insert(table).values(values)
    # compiled_stmt = insert_stmt.compile(compile_kwargs={"literal_binds": True})
    # insert_script += str(compiled_stmt) + ";\n"
    log("(skipped)")

    #### GENERATE FROM XML
    log("Generate CSV...")
    insert_script = ""
    counter = 0
    delimiter = '|'
    quotechar = '"'

    for table in tables.values():
        with open(f'output/{filename_plain}.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=delimiter, quotechar=quotechar, quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerow(all_rows)

            for col in xml_root:
                column = []
                for row in all_rows:
                    val = col.find(row)
                    if val is not None:
                        column.append(val.text)
                    else:
                        column.append("")
                csvwriter.writerow(column)
                counter += 1
                if counter % 50000 == 0:
                    log(f"... processed {counter} rows")

    log(f"... processed {counter} rows in total")
    log(f"File output/{filename_plain}.csv created")
    log("Done!")
