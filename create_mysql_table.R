##############################################################
## Sydeaka Watson, Ph.D.
## Senior Data Scientist, AT&T Chief Data Office
## Lead Data Scient & Owner, Korelasi Data Insights, L.L.C.
## Date of last update: October 8, 2018 
## 
## This function uses the fread function in the data.table package
## to detect column types in a delimited text file 
## and uses the result to construct a CREATE TABLE statement
## that imports the contents of the file into a MYSQL table.
## Optionally, if the user has an active MYSQL session, the user may elect to execute the MYSQL script.
## 
## Inputs:
##   - infile_path: local file directory that contains the file to be imported
##   - sql_path: Local directory where the SQL script will be created
##   - infile_name: name of the file to be imported (without the directory prefix)
##   - mysql_db: MYSQL database where table will be created. 
##        Note: Leave blank if mysql_table_name is a fully specied table name including the database
##   - mysql_table_name: Name of the MYSQL table to be created in mysql_db
##   - mysql_file_name: Name of the MYSQL script to be created
##   - mysql_app: Full path to the mysql application (e.g., /path/to/mysql/bin/mysql)
##   - mysql_username: username for MYSQL authentication
##   - mysql_password: password for MYSQL authentication
##   - preview_size: If use_full_dataset == F, this is the number of rows used for detection of column types.
##   - use_file_dataset: If set to FALSE, use a subset of the rows (preview_size) for detecting data types
##   - create_table: If set to TRUE, use keytab for Kerberos authentication and execute the Hive script
##
## Outputs: NONE
##
## Required package(s): data.table
##
##############################################################


library(data.table)

createMySQLTable = function(infile_path='.', sql_path='.', infile_name=NULL, 
                           mysql_db='db_name', mysql_table_name='table_name', 
                           mysql_file_name='mysql_script.sql', 
                           mysql_app, mysql_username, mysql_password,
                           preview_size=100000, use_full_dataset=F, create_table=F
) {

  ## File in local working directory, to be pushed into HDFS and Hive
  path_to_file = paste(infile_path, infile_name, sep='/')
  
  ## Hive script (i.e., file to be created in this script) that creates the Hive table
  mysql_file = paste(sql_path, mysql_file_name, sep='/')
  
  ## Get number of records
  cmd = paste('cat ', path_to_file, '| wc -l')
  (num_records = as.numeric(trimws(system(cmd, intern=T))) - 1)
  
  ## Read in the file: First pass on a subset to infer the data types
  z = fread(path_to_file, strip.white=F, logical01=F, data.table=F, stringsAsFactors=F, skip=1, #fill=T, comment.char = '#'
            nrows=min(preview_size, num_records))
  
  ## Data types (classes)
  classes = sapply(z, class)
  
  ## Column names
  column_names = colnames(z)
  
  ## Read in the file: Second pass on full dataset to correct the data types
  if (use_full_dataset==T) {
    z = fread(path_to_file, strip.white=F, logical01=F, data.table=F, stringsAsFactors=F, skip=1, #fill=T, comment.char = '#'
              colClasses=classes)
  }
  
  ## Check fields coded as logical data type; See if they are all na
  (logicals = names(classes[classes=='logical']))
  all_na = which(sapply(z[,logicals], function(u) all(is.na(u))) == T)
  not_all_na = which(sapply(z[,logicals], function(u) all(is.na(u))) == F)
  
  if (length(all_na) > 0) {
    classes[logicals[all_na]] = 'integer'
  }
  
  if (length(not_all_na) > 0) {
    classes[logicals[not_all_na]]  = 'character'
  }
  
  
  ## Map R data types to Hive data types
  dtypes = sapply(classes, function(u) {
    if (u=='integer') return('int')
    if (u=='integer64') return('bigint')
    if (u %in% c('double', 'numeric')) return('decimal')
    if (u=='character') return('varchar')
    cat(u, ' Unknown data type. Casting to varchar')
    return('varchar')
  })
  
  ## Detect character lengths
  chars = names(dtypes[dtypes=='varchar'])
  if (length(chars) > 0) {
  	num_chars = sapply(z[,chars], function(u) max(nchar(u)))
  	num_chars[is.na(num_chars)] = 100
  	dtypes[dtypes=='varchar'] = paste(dtypes[dtypes=='varchar'], '(', num_chars, ')', sep='')
  }
  
  ## Decimal sizes
  right_digits = 2
  decimals = names(dtypes[dtypes=='decimal'])
  if (length(decimals) > 0) {
  	#cat('Decimal attributes\n:'); print(decimals)
  	num_digits = sapply(z[,decimals], function(u) max(nchar(trunc(u)), na.rm=T))
  	#cat('Number of digits in decmals\n:'); print(num_digits)
  	num_digits[is.na(num_digits)] = 10
  	num_digits = num_digits + 2
  	num_digits = pmax(num_digits, right_digits)
  	dtypes[dtypes=='decimal'] = paste(dtypes[dtypes=='decimal'], '(', num_digits, ',', right_digits, ')', sep='')
  }
  
  special_names = c('desc', 'asc', 'as')
  id_special = which(names(dtypes) %in% special_names)
  if (length(id_special) > 0) names(dtypes)[id_special] = paste(names(dtypes)[id_special], 'X', sep='_')

  
  ## Attach data types to field names
  field_names_and_types = sapply(1:length(dtypes), function(i) paste(names(dtypes)[i], dtypes[i]))
  field_names_and_types = paste(field_names_and_types, collapse=', \n'); #cat(field_names_and_types)
  
  
  #field_names = paste('(row_num, ', paste(names(dtypes), collapse=', '), ')')
  field_names = paste('(', paste(names(dtypes), collapse=', '), ')')
  
  
  ## Create the script
  cat(
    "SET GLOBAL local_infile=1;
    USE ", mysql_db, ";\n",
    "DROP TABLE IF EXISTS ", mysql_table_name, ";\n",
    "CREATE TABLE ", mysql_table_name, "\n",
    "(",
    #"row_num INT NOT NULL AUTO_INCREMENT, \n",
    field_names_and_types,
    #", PRIMARY KEY (row_num)", 
    ");\n",
    "\nLOAD DATA LOCAL INFILE '", path_to_file, "'
    INTO TABLE ", mysql_table_name, " 
    FIELDS TERMINATED BY ',' 
    OPTIONALLY ENCLOSED BY '\"'
    LINES TERMINATED BY '\\n' 
    IGNORE 2 ROWS 
    ", field_names, "
    ;\n",
    file=mysql_file,
    sep="")
  

  if (create_table==T) {

    ${mysql_run} --user=${user} --password=${password} < data/sql/${mysql_file}
    
    ## Execute the script
    cmd = paste(mysql_app, "--user=", mysql_username, "--password=", mysql_password, "<", mysql_file)
    system(cmd)
    
  } else {
    cat('MYSQL script successfully generated:\n  '
        , mysql_file
        ,'\nSkipping execution of script. Abort.\n')
  }
  
  
} # end createMySQLTable function

# ## Example to create MYSQL table from Lending Club 2018 Q2 dataset
# filename = "LoanStats_2018Q2.csv"
# createMySQLTable(infile_path='./data', infile_name=filename, 
#                 mysql_table_name='mysql_db.LoanStats2018Q2', 
#                 mysql_app=/usr/bin/mysql, mysql_username='my_user', mysql_password='my_pass',
#                 mysql_file_name=gsub('csv', 'sql', paste('mysql_script_', filename, sep='')))


