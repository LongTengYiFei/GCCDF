# grep "old container read amplification:" *.log | awk '{print $5}'
grep "old container read amplification:" *.log | awk '{print $5}' | awk '{if (NR%4==1) print "0"; print $0}'