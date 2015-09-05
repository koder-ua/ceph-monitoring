BEGIN{
  if (divisor=="")
    divisor=1
  if (name_offs=="")
    name_offs=1
}
{
  if( NR == 1 )
  {
     s1_0 = $1
     s2_0 = $2
  }
  s1=$1
  s2=$2
}
END {
  print substr(FILENAME, name_offs, 6), (s1-s1_0)/divisor, (s2-s2_0)/divisor
}
