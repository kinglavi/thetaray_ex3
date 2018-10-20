
# Execute - 
The code should run from the main file.

# Workflows -
There is one workflow that extract shipment data according to the tasks.
At the end of the run the result dataframe will be printed to console.

# Notice - 
I used local spark to run the code.

# Results - 
The initial data is random so here the last run data frame -
<pre>
+-----+-----+------+---------+------+---------+------+---------+------+---------+------+---------+-------------------+-------+
|index|name |dest1 |duretion1|dest2 |duretion2|dest3 |duretion3|dest4 |duretion4|dest5 |duretion5|tx_date            |balance|
+-----+-----+------+---------+------+---------+------+---------+------+---------+------+---------+-------------------+-------+
|0    |Arika|ISRAEL|3        |SPAIN |16       |null  |41       |SWEDEN|14       |ISRAEL|6        |2018-01-01 10:12:14|1.1    |
|1    |Bob  |SPAIN |21       |SWEDEN|3        |SWEDEN|29       |SPAIN |30       |SWEDEN|44       |2018-01-06 10:12:14|2.2    |
|2    |Corry|SPAIN |10       |null  |15       |ISRAEL|26       |SWEDEN|30       |null  |29       |2018-01-11 10:12:14|3.3    |
|3    |David|SWEDEN|50       |null  |16       |SWEDEN|41       |null  |42       |SPAIN |35       |2018-01-16 10:12:14|4.4    |
|4    |Trump|SWEDEN|19       |SWEDEN|50       |SPAIN |32       |SWEDEN|15       |ISRAEL|5        |2018-01-21 10:12:14|1.1    |
|5    |Arika|ISRAEL|38       |null  |13       |null  |9        |SWEDEN|10       |SPAIN |9        |2018-01-26 10:12:14|2.2    |
|6    |Bob  |ISRAEL|9        |ISRAEL|39       |null  |10       |null  |25       |null  |28       |2018-01-31 10:12:14|3.3    |
|7    |Corry|ISRAEL|32       |null  |18       |null  |46       |null  |44       |SWEDEN|5        |2018-02-05 10:12:14|4.4    |
|8    |David|ISRAEL|18       |null  |37       |SWEDEN|1        |SWEDEN|31       |SWEDEN|45       |2018-02-10 10:12:14|1.1    |
|9    |Trump|SPAIN |24       |null  |17       |SPAIN |14       |null  |23       |null  |50       |2018-02-15 10:12:14|2.2    |
|10   |Arika|ISRAEL|20       |null  |33       |SWEDEN|41       |null  |43       |SWEDEN|32       |2018-02-20 10:12:14|3.3    |
|11   |Bob  |SPAIN |33       |SPAIN |41       |SWEDEN|3        |null  |46       |SWEDEN|33       |2018-02-25 10:12:14|4.4    |
|12   |Corry|SWEDEN|49       |SPAIN |2        |ISRAEL|11       |ISRAEL|38       |SWEDEN|31       |2018-03-02 10:12:14|1.1    |
|13   |David|ISRAEL|16       |SPAIN |37       |null  |10       |SWEDEN|21       |SPAIN |33       |2018-03-07 10:12:14|2.2    |
|14   |Trump|ISRAEL|7        |SPAIN |45       |SPAIN |22       |ISRAEL|26       |SWEDEN|49       |2018-03-12 10:12:14|3.3    |
|15   |Arika|SWEDEN|34       |ISRAEL|37       |SWEDEN|40       |ISRAEL|23       |SWEDEN|14       |2018-03-17 10:12:14|4.4    |
|16   |Bob  |ISRAEL|13       |SPAIN |44       |null  |15       |SPAIN |49       |SPAIN |26       |2018-03-22 10:12:14|1.1    |
|17   |Corry|SWEDEN|43       |null  |49       |SPAIN |1        |null  |48       |ISRAEL|14       |2018-03-27 10:12:14|2.2    |
|18   |David|SWEDEN|22       |SPAIN |20       |null  |48       |null  |45       |SPAIN |9        |2018-04-01 10:12:14|3.3    |
|19   |Trump|SWEDEN|30       |null  |34       |ISRAEL|7        |SWEDEN|13       |null  |17       |2018-04-06 10:12:14|4.4    |
+-----+-----+------+---------+------+---------+------+---------+------+---------+------+---------+-------------------+-------+
</pre>

And the results data frame (from the above data) is - 

<pre>
|index|name |tx_date            |balance|full_path                         |total_duration|avg_duration_to_israel|avg_duration_from_israel|
+-----+-----+-------------------+-------+----------------------------------+--------------+----------------------+------------------------+
|0    |Arika|2018-01-01 10:12:14|1.1    |ISRAEL,SPAIN,SWEDEN,ISRAEL        |80            |1                     |2                       |
|1    |Bob  |2018-01-06 10:12:14|2.2    |SPAIN,SWEDEN,SWEDEN,SPAIN,SWEDEN  |127           |0                     |0                       |
|2    |Corry|2018-01-11 10:12:14|3.3    |SPAIN,ISRAEL,SWEDEN               |110           |5                     |5                       |
|3    |David|2018-01-16 10:12:14|4.4    |SWEDEN,SWEDEN,SPAIN               |184           |0                     |0                       |
|4    |Trump|2018-01-21 10:12:14|1.1    |SWEDEN,SWEDEN,SPAIN,SWEDEN,ISRAEL |121           |1                     |0                       |
|5    |Arika|2018-01-26 10:12:14|2.2    |ISRAEL,SWEDEN,SPAIN               |79            |7                     |1                       |
|6    |Bob  |2018-01-31 10:12:14|3.3    |ISRAEL,ISRAEL                     |111           |9                     |8                       |
|7    |Corry|2018-02-05 10:12:14|4.4    |ISRAEL,SWEDEN                     |145           |6                     |2                       |
|8    |David|2018-02-10 10:12:14|1.1    |ISRAEL,SWEDEN,SWEDEN,SWEDEN       |132           |3                     |6                       |
|9    |Trump|2018-02-15 10:12:14|2.2    |SPAIN,SPAIN                       |128           |0                     |0                       |
|10   |Arika|2018-02-20 10:12:14|3.3    |ISRAEL,SWEDEN,SWEDEN              |169           |4                     |5                       |
|11   |Bob  |2018-02-25 10:12:14|4.4    |SPAIN,SPAIN,SWEDEN,SWEDEN         |156           |0                     |0                       |
|12   |Corry|2018-03-02 10:12:14|1.1    |SWEDEN,SPAIN,ISRAEL,ISRAEL,SWEDEN |131           |9                     |12                      |
|13   |David|2018-03-07 10:12:14|2.2    |ISRAEL,SPAIN,SWEDEN,SPAIN         |117           |3                     |6                       |
|14   |Trump|2018-03-12 10:12:14|3.3    |ISRAEL,SPAIN,SPAIN,ISRAEL,SWEDEN  |149           |6                     |17                      |
|15   |Arika|2018-03-17 10:12:14|4.4    |SWEDEN,ISRAEL,SWEDEN,ISRAEL,SWEDEN|148           |12                    |9                       |
|16   |Bob  |2018-03-22 10:12:14|1.1    |ISRAEL,SPAIN,SPAIN,SPAIN          |147           |2                     |7                       |
|17   |Corry|2018-03-27 10:12:14|2.2    |SWEDEN,SPAIN,ISRAEL               |155           |2                     |0                       |
|18   |David|2018-04-01 10:12:14|3.3    |SWEDEN,SPAIN,SPAIN                |144           |0                     |0                       |
|19   |Trump|2018-04-06 10:12:14|4.4    |SWEDEN,ISRAEL,SWEDEN              |101           |1                     |1                       |
+-----+-----+-------------------+-------+----------------------------------+--------------+----------------------+------------------------+
</pre>