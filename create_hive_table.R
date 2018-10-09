##############################################################
## Sydeaka Watson, Ph.D.
## Senior Data Scientist, AT&T Chief Data Office
## Lead Data Scient & Owner, Korelasi Data Insights, L.L.C.
## Date of last update: October 8, 2018 
## 
## This function uses the fread function in the data.table package
## to detect column types in a delimited text file 
## and uses the result to construct a CREATE TABLE statement
## that imports the contents of the file into a Hive table.
## Optionally, if the user has an active Hive session, the user may elect to execute the Hive script.
## 
## Inputs:
##   - infile_path: local file directory that contains the file to be imported 
##   - infile_name: name of the file to be imported (without the directory prefix)
##   - hdfs_file_path: HDFS directory in which to temporarily store the data file
##   - hive_db: Hive database where table will be located. 
##        Note: Leave blank if hive_table_name is a fully specied table name including the database
##   - hive_table_name: Name of the Hive table to be created in hive_db
##   - hive_file_name: Name of the Hive script to be created
##   - preview_size: If use_full_dataset == F, this is the number of rows used for detection of column types.
##   - use_file_dataset: If set to FALSE, use a subset of the rows (preview_size) for detecting data types
##   - path_to_keytab: Path to keytab for Kerberos authentication.
##   - create_table: If set to TRUE, use keytab for Kerberos authentication and execute the Hive script
##
## Outputs: NONE
##
## Required package(s): data.table
##
##############################################################



library(data.table)


createHiveTable = function(infile_path='.', infile_name=NULL, hdfs_file_path=NULL,
                           hive_db='', hive_table_name=NULL, hive_file_name='hive_script.hql',
                           preview_size=100000, use_full_dataset=F, path_to_keytab=NULL, 
                           create_table=F
                           ) {

  ## Fully specified Hive table name
  hive_table_name = paste0(hive_db, hive_table_name)

  ## File in local working directory, to be pushed into HDFS and Hive
  path_to_file = paste(infile_path, infile_name, sep='/')
  
  ## File to be created in HDFS (copy of infile)
  hdfs_file = paste(hdfs_file_path, infile_name, sep='/')
  
  ## Hive script (i.e., file to be created in this script) that creates the Hive table
  hive_file = paste(infile_path, hive_file_name, sep='/')
  
  ## Get number of records
  cmd = paste('cat ', path_to_file, '| wc -l')
  (num_records = as.numeric(trimws(system(cmd, intern=T))) - 1)
  
  ## Read in the file: First pass on a subset to infer the data types
  z = fread(path_to_file, strip.white=F, logical01=F, data.table=F, stringsAsFactors=F,
            nrows=min(preview_size, num_records))
  
  ## Data types (classes)
  classes = sapply(z, class)
  
  ## Column names
  column_names = colnames(z)
  
  ## Read in the file: Second pass on full dataset to correct the data types
  if (use_full_dataset==T) {
    z = fread(path_to_file, strip.white=F, logical01=F, data.table=F, stringsAsFactors=F,
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
  num_chars = sapply(z[,chars], function(u) max(nchar(u)))
  num_chars[is.na(num_chars)] = 100
  dtypes[dtypes=='varchar'] = paste(dtypes[dtypes=='varchar'], '(', num_chars, ')', sep='')
  
  ## Decimal sizes
  decimals = names(dtypes[dtypes=='decimal'])
  num_digits = sapply(z[,decimals], function(u) max(nchar(trunc(u)), na.rm=T))
  num_digits[is.na(num_digits)] = 10
  dtypes[dtypes=='decimal'] = paste(dtypes[dtypes=='decimal'], '(', num_digits, ',3)', sep='')
  
  ## Attach data types to field names
  field_names_and_types = sapply(1:length(dtypes), function(i) paste(names(dtypes)[i], dtypes[i]))
  field_names_and_types = paste(field_names_and_types, collapse=', \n'); #cat(field_names_and_types)
  

  ## Create the Hive script
  cat(
    "DROP TABLE ", hive_table_name, ";\n",
    "CREATE TABLE ", hive_table_name, "\n",
    "(",
    field_names_and_types,
    ") row format delimited fields terminated by ','  stored as textfile;\n",
    "LOAD DATA INPATH '", hdfs_file, "' OVERWRITE INTO TABLE ", hive_table_name, ";\n",
    file=hive_file,
    sep="")
  
  if (create_table==T) {
  
    ## Put the data into HDFS
    cmd = "hdfs dfs -put /opt/data/share05/sandbox/sandbox7/sw659h/demo/workflowautomation/data/es_dtv_dispatch_mobility_aci.csv /sandbox/sandbox7/sw659h/opx/data"
    system(cmd)
    
    ## Kerberos authentication
    if (is.null(path_to_keytab)) {
      cat('Keytab not specified. Hive script cannot run without Kerberos authentication. Exiting the script.')
      return(NULL)
    } 
    
    ## Execute the Hive script
    cmd = paste('hive -f', hive_file)
    system(cmd)
  
  } else {
    cat('Skipping execution of hive script. Abort.\n')
  }
  
  
} # end createHiveTable function


# ## Example to create Hive table from Lending Club 2018 Q2 dataset
# filename = "LoanStats_2018Q2.csv"
# createHiveTable(infile_path='./data', infile_name=filename, 
#                 hive_table_name='mydbname.LoanStats2018Q2', 
#                 hive_file_name=gsub('csv', 'hql', paste('hive_script_', 
#                   filename, sep='')))


