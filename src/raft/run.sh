
for ((i=0; i<1000; i++))
do
  echo "$i"
  #go test -run="TestCount" > ${i}.log 2>&1
  #go test -run="TestFigure8" > ${i}.log 2>&1
  #go test -run="TestFigure8Unreliable" > ${i}.log 2>&1
  #go test -run="TestReliableChurn" > ${i}.log 2>&1
  #res=`cat ${i}.log | grep PASS`
  #if [ "$res" = "PASS" ]
  #then 
  #   echo $res
  #   rm  ${i}.log
  #fi
  go test >> report.log 2>&1
done
