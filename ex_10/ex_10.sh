#!/bin/bash
#   CHANGELOG
#   script for loading data, validating and export data to csv format
#   25/01/2021
URL="http://home.agh.edu.pl/~wsarlej/dyd/bdp2/materialy/cw10/InternetSales_new.zip"
#URL_OLD="http://home.agh.edu.pl/~wsarlej/dyd/bdp2/materialy/cw10/InternetSales_old.txt"
FILENAME=$(basename $URL)
PASSWORD="YmRwMmFnaAo=" #base64
BASE="${FILENAME%.*}"
FILETXT="${BASE}.txt"
FILEBAD="${BASE}_bad.txt"
LOGFILE="log.txt"
STUDENT_NUMBER="290936"
TIMESTAMP="$(date "+%m%d%Y")"
EMAIL="krzysztofstrzalkowski27@gmail.com"

SQL_DB="mysql"
SQL_USER="290936"
SQL_PASSWORD="YmF6eV8yMDIxCg==" #base64


mkdir PROCESSED
touch PROCESSED/$LOGFILE

#download file InternetSales_new
wget "$URL"
EXITCODE=$?
if [ $EXITCODE -eq 0 ]
then
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Downloading file - SUCCESS" >> PROCESSED/"$LOGFILE"
else
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Downloading file - FAILED" >> PROCESSED/"$LOGFILE"
    
fi

#unzip InternetSales_new
unzip -oP "$(base64 -d <<< "$PASSWORD")" "$FILENAME"
EXITCODE=$?
if [ $EXITCODE -eq 0 ]
then
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Unzipping file - SUCCESS" >> PROCESSED/"$LOGFILE"
else
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Unzipping file - FAILED" >> PROCESSED/"$LOGFILE"
    
fi

rm -f -- "$FILENAME"

#validate file
number_lines=$(wc -l < "$FILETXT")
temp="$(mktemp)"

#remove empty lines
sed -i '/^$/d' "$FILETXT" >> $temp

EXITCODE=$?
if [ $EXITCODE -eq 0 ]
then
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Removing empty lines - SUCCESS" >> PROCESSED/"$LOGFILE"
else
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Removing empty lines - FAILED" >> PROCESSED/"$LOGFILE"
fi

without_empty_lines=$(wc -l < "$FILETXT")
empty_lines=$(($number_lines - $without_empty_lines))
duplicated_rows=$(sort "$FILETXT" | uniq -D | wc -l)

awk -v badfile="${FILEBAD}" '
	NR == FNR {duplicated_rows[$0]++; next}
	FNR == 1 {colums_number=NF;next}
	$colums_number!="" {$colums_number=""; print > badfile; next}
	duplicated_rows[$0]++ {print > badfile; next}
	$5 > 100 {print > badfile; next}
	{
        for(i=1; i<NF;i++){
			if($i == ""){
                print > badfile;
				next;
			}
		}
		if(match($3,/^"([[:alpha:] -]+),([[:alpha:] -]+)"$/, pattern_tab)){
			$3=pattern_tab[2] OFS pattern_tab[1]
			print $0
		}
		else {
			print > badfile
		}
	}
' FS='|' OFS='|' "InternetSales_old.txt" "$FILETXT"  > "$temp"
mv "$temp" "$FILETXT"

EXITCODE=$?
if [ $EXITCODE -eq 0 ]
then
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Parsing file - SUCCESS" >> PROCESSED/"$LOGFILE"
else
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Parsing file - FAILED" >> PROCESSED/"$LOGFILE"
fi

correct_rows=$(wc -l < "$FILETXT")
incorrect_rows=$(wc -l < "$FILEBAD")

query_create="CREATE TABLE CUSTOMERS_$STUDENT_NUMBER
(ProductKey INT, CurrencyAlternateKey VARCHAR(4), FirstName VARCHAR(255), LastName VARCHAR(255),
OrderDateKey VARCHAR(30), OrderQuantity INT, UnitPrice FLOAT, SecretCode VARCHAR(30));"

query_insert="LOAD DATA LOCAL INFILE '$FILETXT' 
INTO TABLE CUSTOMERS_$STUDENT_NUMBER
FIELDS TERMINATED BY '|' 
ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;"

mysql -u "$SQL_USER" -p"$(base64 -d <<< "$SQL_PASSWORD")" "$SQL_DB" -e "$query_create"

EXITCODE=$?
if [ $EXITCODE -eq 0 ]
then
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Creating table - SUCCESS" >> PROCESSED/"$LOGFILE"
else
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Creating table - FAILED" >> PROCESSED/"$LOGFILE"
fi

mysql -u "$SQL_USER" -p"$(base64 -d <<< "$SQL_PASSWORD")" "$SQL_DB" -e "$query_insert"

EXITCODE=$?
if [ $EXITCODE -eq 0 ]
then
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Inserting values - SUCCESS" >> PROCESSED/"$LOGFILE"
else
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Inserting values - FAILED" >> PROCESSED/"$LOGFILE"
fi


mv "${FILETXT}" PROCESSED/"${TIMESTAMP}${FILETXT}"

#send mail

mail_body="$(printf "%s\n%s\n%s\n%s\n\n" "$without_empty_lines" "$correct_rows" "$duplicated_rows" "$incorrect_rows")"
mail -s "CUSTOMERS_LOAD - ${TIMESTAMP}" "$EMAIL" <<< "$mail_body"

EXITCODE=$?
if [ $EXITCODE -eq 0 ]
then
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Sending mail - SUCCESS" >> PROCESSED/"$LOGFILE"
else
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Sending mail - FAILED" >> PROCESSED/"$LOGFILE"
fi


#update table
query_update="UPDATE CUSTOMERS_$STUDENT_NUMBER set SecretCode = ( SELECT substring(MD5(RAND()), -10) );"
mysql -u "$SQL_USER" -p"$(base64 -d <<< "$SQL_PASSWORD")" "$SQL_DB" -e "$query_update"

EXITCODE=$?
if [ $EXITCODE -eq 0 ]
then
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Updating secret codes - SUCCESS" >> PROCESSED/"$LOGFILE"
else
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Updating secret codes - FAILED" >> PROCESSED/"$LOGFILE"
fi


#export data from table to csv
temp=$(mktemp -u)
query_export="SELECT * FROM CUSTOMERS_$STUDENT_NUMBER
INTO OUTFILE '$temp' 
FIELDS ENCLOSED BY '\"' 
TERMINATED BY ',' 
ESCAPED BY '\"' 
LINES TERMINATED BY '\n';"


mysql -u "$SQL_USER" -p"$(base64 -d <<< "$SQL_PASSWORD")" "$SQL_DB" -e "$query_export"
cp "$temp" "customers.csv"

EXITCODE=$?
if [ $EXITCODE -eq 0 ]
then
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Exporting table - SUCCESS" >> PROCESSED/"$LOGFILE"
else
    TIME=$(date +"%m-%d-%y %T")
    echo "$TIME : Exporting table - FAILED" >> PROCESSED/"$LOGFILE"
fi

#zip file to tar.gz

tar -zcf customers.tar.gz customers.csv
mail -s "CUSTOMERS_EXPORT - ${TIMESTAMP}" -acustomers.tar.gz "$EMAIL" <<< "$(printf "%s\n" "$TIMESTAMP")"
