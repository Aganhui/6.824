# sort -t ' ' -k2 -n mr-out-dis > mr-out-dis-num
if [ $# -eq 0 ]; then
  mr1=mr-out-dis
  mr2=mr-out-seq
fi
if [ $# -eq 2 ]; then
  mr1=$1
  mr2=$2
fi
if cmp $mr1 $mr2
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi
