#* * * * * /usr/bin/uptime >> /tmp/uptime.oozie.txt

#ybrdev93
OOZIE_HOME=/var/lib/oozie
10 8 * * 1-5 sh /usr/book/app/yb-apache-hbase/src/main/scripts/fhlmc/RepeatDailyMonthlyLoanProcess.sh >> /tmp/fhlmc_loan_daily.log 2>&1
0 19 * * 1-5 sh /usr/book/app/yb-apache-hbase/src/main/scripts/fhlmc/RepeatMonthlyLoanProcess.sh >> /tmp/fhlmc_loan_monthly.log 2>&1
~
#ybrdev79
OOZIE_HOME=/var/lib/oozie
30 7 * * 2-6 sh /usr/book/app/yb-bigdata/src/main/scripts/fnma/RepeatDailyMonthlyPoolProcess.sh >> /tmp/fnma_pool_daily.log 2>&1
40 7 * * 2-6 sh /usr/book/app/yb-apache-hbase/src/main/scripts/gnma/RepeatDailyMonthlyLoanProcess.sh >> /tmp/gnma_loan_daily.log 2>&1
30 19 * * 2-6 sh /usr/book/app/yb-apache-hbase/src/main/scripts/gnma/RepeatMonthlyLoanProcess.sh >> /tmp/gnma_loan_monthly.log 2>&1
55 7  * * 2-6 sh /usr/book/app/yb-apache-hbase/src/main/scripts/fnma/RepeatDailyMonthlyLoanProcess.sh >> /tmp/fnma_loan_daily.log 2>&1
0 19 * * 2-6 sh /usr/book/app/yb-apache-hbase/src/main/scripts/fnma/RepeatMonthlyLoanProcess.sh >> /tmp/fnma_loan_monthly.log 2>&1


#ybgdev93 install anaconda3: