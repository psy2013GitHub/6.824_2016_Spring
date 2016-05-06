
$case=$1
go test -run=\"$case\" > report.txt 2>&1; tail -n 1 report.txt 
