# SQL-Hive-Import-Utilities
Each of these functions use the fread function in the data.table package to detect column types in a delimited text file and use the result to construct a CREATE TABLE statement that imports the contents of the file into a Hive/MYSQL table. Optionally, if the user has an active Hive/MYSQL session, the user may elect to execute the script.
