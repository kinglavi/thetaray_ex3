
# Execute - 
The code should run from the main file.

# Workflows -
There is one workflow that extract shipment data according to the tasks. <br/>
At the end of the run the result dataframe will be printed to console.

# Columns -
full_path - The full path of the shipment split by comma. <br/> 
total_duration - The total duration of each shipment. <br/>
avg_duration_to_israel - The average duration from each country to israel. <br/>
avg_duration_from_israel - The average duration from israel to any country. <br/>

# Notice - 
I used local spark to run the code.

# Results - 
The initial data is random so here the last run data frame -
<pre>
+-----+-----+------+---------+------+---------+------+---------+------+---------+------+---------+-------------------+-------+
|index|name |dest1 |duretion1|dest2 |duretion2|dest3 |duretion3|dest4 |duretion4|dest5 |duretion5|tx_date            |balance|
+-----+-----+------+---------+------+---------+------+---------+------+---------+------+---------+-------------------+-------+
|0    |Arika|SWEDEN|49       |ISRAEL|28       |null  |26       |SPAIN |42       |SWEDEN|20       |2018-01-01 10:12:14|1.1    |
|1    |Bob  |SPAIN |14       |SPAIN |34       |SWEDEN|19       |ISRAEL|22       |SPAIN |1        |2018-01-06 10:12:14|2.2    |
|2    |Corry|SPAIN |16       |SPAIN |42       |SPAIN |39       |null  |29       |null  |29       |2018-01-11 10:12:14|3.3    |
|3    |David|ISRAEL|9        |SWEDEN|15       |SPAIN |26       |ISRAEL|24       |SWEDEN|1        |2018-01-16 10:12:14|4.4    |
|4    |Trump|SPAIN |3        |SPAIN |31       |SWEDEN|37       |null  |14       |ISRAEL|35       |2018-01-21 10:12:14|1.1    |
|5    |Arika|SPAIN |31       |ISRAEL|3        |SPAIN |33       |null  |13       |SWEDEN|35       |2018-01-26 10:12:14|2.2    |
|6    |Bob  |SPAIN |35       |SWEDEN|31       |ISRAEL|41       |ISRAEL|6        |null  |4        |2018-01-31 10:12:14|3.3    |
|7    |Corry|ISRAEL|49       |SPAIN |16       |ISRAEL|10       |null  |49       |SPAIN |32       |2018-02-05 10:12:14|4.4    |
|8    |David|SPAIN |45       |ISRAEL|1        |null  |22       |SPAIN |2        |ISRAEL|33       |2018-02-10 10:12:14|1.1    |
|9    |Trump|SPAIN |22       |null  |10       |ISRAEL|24       |SWEDEN|25       |SPAIN |27       |2018-02-15 10:12:14|2.2    |
|10   |Arika|SPAIN |14       |null  |9        |SWEDEN|10       |null  |49       |SWEDEN|21       |2018-02-20 10:12:14|3.3    |
|11   |Bob  |ISRAEL|8        |SWEDEN|49       |SPAIN |44       |SPAIN |41       |null  |10       |2018-02-25 10:12:14|4.4    |
|12   |Corry|SWEDEN|30       |ISRAEL|27       |SPAIN |42       |SWEDEN|26       |SPAIN |20       |2018-03-02 10:12:14|1.1    |
|13   |David|SWEDEN|15       |ISRAEL|10       |ISRAEL|27       |SPAIN |33       |ISRAEL|26       |2018-03-07 10:12:14|2.2    |
|14   |Trump|SWEDEN|47       |SWEDEN|3        |ISRAEL|9        |null  |3        |null  |1        |2018-03-12 10:12:14|3.3    |
|15   |Arika|SWEDEN|48       |SWEDEN|38       |SPAIN |5        |null  |50       |null  |43       |2018-03-17 10:12:14|4.4    |
|16   |Bob  |SPAIN |22       |SPAIN |1        |ISRAEL|26       |SWEDEN|43       |ISRAEL|41       |2018-03-22 10:12:14|1.1    |
|17   |Corry|SPAIN |32       |ISRAEL|27       |SWEDEN|4        |SWEDEN|13       |SPAIN |45       |2018-03-27 10:12:14|2.2    |
|18   |David|ISRAEL|8        |SPAIN |26       |SWEDEN|11       |null  |6        |null  |44       |2018-04-01 10:12:14|3.3    |
|19   |Trump|SPAIN |6        |SWEDEN|4        |ISRAEL|47       |SWEDEN|24       |SPAIN |49       |2018-04-06 10:12:14|4.4    |
+-----+-----+------+---------+------+---------+------+---------+------+---------+------+---------+-------------------+-------+
</pre>

And the results data frame (from the above data) is - 

<pre>
+-----+-----+-------------------+-------+---------------------------------+--------------+----------------------+------------------------+
|index|name |tx_date            |balance|full_path                        |total_duration|avg_duration_to_israel|avg_duration_from_israel|
+-----+-----+-------------------+-------+---------------------------------+--------------+----------------------+------------------------+
|0    |Arika|2018-01-01 10:12:14|1.1    |SWEDEN,ISRAEL,SPAIN,SWEDEN       |165           |28                    |26                      |
|1    |Bob  |2018-01-06 10:12:14|2.2    |SPAIN,SPAIN,SWEDEN,ISRAEL,SPAIN  |90            |22                    |1                       |
|2    |Corry|2018-01-11 10:12:14|3.3    |SPAIN,SPAIN,SPAIN                |155           |0                     |0                       |
|3    |David|2018-01-16 10:12:14|4.4    |ISRAEL,SWEDEN,SPAIN,ISRAEL,SWEDEN|75            |16                    |8                       |
|4    |Trump|2018-01-21 10:12:14|1.1    |SPAIN,SPAIN,SWEDEN,ISRAEL        |120           |35                    |0                       |
|5    |Arika|2018-01-26 10:12:14|2.2    |SPAIN,ISRAEL,SPAIN,SWEDEN        |115           |3                     |33                      |
|6    |Bob  |2018-01-31 10:12:14|3.3    |SPAIN,SWEDEN,ISRAEL,ISRAEL       |117           |23                    |5                       |
|7    |Corry|2018-02-05 10:12:14|4.4    |ISRAEL,SPAIN,ISRAEL,SPAIN        |156           |29                    |32                      |
|8    |David|2018-02-10 10:12:14|1.1    |SPAIN,ISRAEL,SPAIN,ISRAEL        |103           |17                    |22                      |
|9    |Trump|2018-02-15 10:12:14|2.2    |SPAIN,ISRAEL,SWEDEN,SPAIN        |108           |24                    |25                      |
|10   |Arika|2018-02-20 10:12:14|3.3    |SPAIN,SWEDEN,SWEDEN              |103           |0                     |0                       |
|11   |Bob  |2018-02-25 10:12:14|4.4    |ISRAEL,SWEDEN,SPAIN,SPAIN        |152           |8                     |49                      |
|12   |Corry|2018-03-02 10:12:14|1.1    |SWEDEN,ISRAEL,SPAIN,SWEDEN,SPAIN |145           |27                    |42                      |
|13   |David|2018-03-07 10:12:14|2.2    |SWEDEN,ISRAEL,ISRAEL,SPAIN,ISRAEL|111           |21                    |30                      |
|14   |Trump|2018-03-12 10:12:14|3.3    |SWEDEN,SWEDEN,ISRAEL             |63            |9                     |3                       |
|15   |Arika|2018-03-17 10:12:14|4.4    |SWEDEN,SWEDEN,SPAIN              |184           |0                     |0                       |
|16   |Bob  |2018-03-22 10:12:14|1.1    |SPAIN,SPAIN,ISRAEL,SWEDEN,ISRAEL |133           |33                    |43                      |
|17   |Corry|2018-03-27 10:12:14|2.2    |SPAIN,ISRAEL,SWEDEN,SWEDEN,SPAIN |121           |27                    |4                       |
|18   |David|2018-04-01 10:12:14|3.3    |ISRAEL,SPAIN,SWEDEN              |95            |8                     |26                      |
|19   |Trump|2018-04-06 10:12:14|4.4    |SPAIN,SWEDEN,ISRAEL,SWEDEN,SPAIN |130           |47                    |24                      |
+-----+-----+-------------------+-------+---------------------------------+--------------+----------------------+------------------------+
</pre>