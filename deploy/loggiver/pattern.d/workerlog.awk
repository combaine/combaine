#!/usr/bin/mawk -f

# worker.log tsv log
# 2017-05-10T23:03:48+03:00 INFO fetching completed (took 0.081) [config: kassa-test-front-nginx, session: 24441b4f88d29ac4f1c99fbd6186f262, target: kassa-front01e.kassa.tst.yandex.net]
# 2017-05-10T23:03:48+03:00 INFO parsing completed (took 0.003)  [config: kassa-test-front-nginx, session: 24441b4f88d29ac4f1c99fbd6186f262, target: kassa-front01e.kassa.tst.yandex.net]
# 2017-05-10T23:03:48+03:00 INFO aggregation completed (took 0.022)      [config: kassa-test-front-nginx, session: 24441b4f88d29ac4f1c99fbd6186f262]
# 2017-05-10T23:03:49+03:00 INFO senders completed (took 0.446)  [config: kassa-test-back-nginx, session: 123878623b31edd1b7f9b28e7645b3ba]


function print_timings(name){
  match_by = name""SUBSEP
  printf "@timings."name"_timings";
  for(i in timings)
    if (index(i, match_by) != 0)
      printf " %.3f@%s", substr(i, index(i, SUBSEP)+1), timings[i]
  print "";
}

BEGIN {
   IFS=/ \t/
}
{
  status[$2]++

  if ($4 == "completed" && $5 =="(took") {
    ++tlens[$3]
    timings[$3, $6]++
  }
}

END {
  for (n in tlens)
    print_timings(n)

  for (n in status){
    print(n, status[n])
  }
}
