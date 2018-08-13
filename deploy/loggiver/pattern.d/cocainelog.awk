#!/usr/bin/mawk -f

{
  if ($1 !~ "^\[") next

  status[$7"."$6]++

  if ($7 == "app/juggler:" && ($0 ~ /failed to send juggler Event/ || $0 ~ /Sending error/))
    sender_timeouts["juggler"]++
  if ($7 == "app/graphite:" && $0 ~ /Sending error/ && $0 !~ /Nothing to send/)
    sender_timeouts["graphite"]++
  if ($7 == "app/solomon:" && $0 ~ /failed to send/ && $0 !~ /Nothing to send/)
    sender_timeouts["solomon"]++
  if ($7 == "app/razladki:" && $0 ~ /unable to do http request/)
    sender_timeouts["razladki"]++
  if ($7 == "app/agave:" && $0 ~ /Unable to do request Get/)
    sender_timeouts["agave"]++

}

END {
  for (n in sender_timeouts)
    print("sender_timeouts."n, sender_timeouts[n])

  for (n in status){
    k = n
    gsub("/", ".", n);
    gsub(":", "", n);
    gsub("\[", "", n);
    gsub("\]", "", n);
    print(n, status[k])
  }
}
