# Gas-monitoring

This is a project as part of my course (MSc. Data Analytics 2015-2016), for "Programming for Data Analysis".

The gas data set is obtained from the UCI machine learning website (http://archive.ics.uci.edu/ml/datasets/Gas+sensor+array+under+dynamic+gas+mixtures), with sensor readings of 16 electronic chemosensors. A gas mixture with varying but controlled concentrations of CO and ethylene is continuously filled in a chamber where the 16 sensors are installed, and the sensor reading are obtained in a nearly real-time manner for 12 hours.

This project is to use MapReduce to 

1. Group the records by seconds, with all the sensor readings averaged across the time points within each time second.
2. With the grouped table, join each row with the 16 sensor readings from the last row (within the preceding time second) and the 16 readings from the row before the last row. That is, the sensor readings of tim t-1 and the time t-2 are joined with the record of time t.

With the two MapReduce tasks completed in Java, the resultant output data are exported into MySQL, and then retrieved and analysed in R.

The two tasks are implemented with the "COtoSecond" and "TimeJoin2" java files, separately. 

The output data are also backuped in Hbase, using the "TimeJoinHbase" java files.

Some data exploration is done with Hive based on the original data on HDFS.
