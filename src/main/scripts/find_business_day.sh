#! /usr/bin/ksh

Day=$(date +%d)
Month=$(date +%m)
Year=$( date +%Y)
HOLIDAY=/home/unxsa/holiday/holiday.date 
AWK=nawk

usage () {
  echo "Holiday date file at $HOLIDAY"
  echo "Usage: $0 Day [ Month Year ] "
  echo "example: "
  echo "get the 5th business day in current month"
  echo "$0 5 "
  echo "get the 10th bueinsee day on May 2010"
  echo "$0 10 5 2010"
}

if [[ $1 == "" ]]; then
  usage
  exit
else
  Bday=$1
fi

if [[ $3 == "" ]]; then
  CAL="/usr/bin/cal"
else
  Month=$2
  Year=$3
  CAL="/usr/bin/cal $Month $Year"
fi

Blist=$( $CAL |$AWK 'NR>2 {print substr($0,4,14)}' |tr "\n" " ")
$AWK -F \/ -v m=$Month -v y=$Year '$2==m&& $3==y {sub(/^0/,"",$1);print $1}' $HOLIDAY |while read line
do
  Blist=$(echo $Blist |$AWK -v d=$line '{for (i=1;i<=NF;i++) $i=($i==d)?"":$i}1' )
done

echo $Blist |$AWK -v d=$Bday '{print $d}'