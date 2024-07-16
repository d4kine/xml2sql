# XML 2 SQL

Application to convert XML files into SQL DDL/DCL statements (MSSQL).

All XML tags are parsed and different rows of data types are generated in relation to the values.

There are two folders in the project, `xml` and `sql`.
The `xml` folder contains the XML files to be converted and the `sql` folder contains the generated SQL files.
Filenames given as arguments must be placed in the `xml` folder first.

## Requirements

The following packages are required to run the application.

- Python 3
  - pyodbc
  - sqlalchemy

- unixodbc (Mac OS X)

## Configuration

The `config.py` file contains the configuration for the application:

```python
custom_strings = {"A", "B", "C"}
custom_floats = {"TEST"}
```

By definig custom strings and floats, the application will generate the SQL DDL/DCL statements with the custom types.
So all XML-elements with the custom strings will be converted to `VARCHAR(255)` and all XML-elements with the custom floats will be converted to `FLOAT`.



## Prerequisites

Install the required Python packages.

```bash
# Mac OS X only
brew install unixodbc

pip install -r requirements
```

## Launch

Execute the main.py in a terminal with the filename as argument.

```bash
python main.py {filename}
```