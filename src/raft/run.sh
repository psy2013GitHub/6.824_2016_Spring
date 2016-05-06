
for ((i=0; i<200; i++))
do
  go test -run="TestFigure8Unreliable" > ${i}.log 2>&1
  echo "$i"
  cat ${i}.log | grep PASS
  #go test >> report.log 2>&1
done
