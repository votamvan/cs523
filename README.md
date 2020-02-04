[Presentation](https://onedrive.live.com/view.aspx?resid=35EA824A9571A24D!418)

### Install Spark 2.4.4 on Hadoop 2.6
```
sh setup_spark2.sh
```

### Setting enviroment $HADOOP_CONF_DIR
```
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
```

### Spark streaming from S3 bucket <air-quality-live>
```
spark-submit --class cs523.App --master yarn ./target/final-1.0-jar-with-dependencies.jar s3
```

### Spark streaming from Kafka server
```
spark-submit --class cs523.App --master yarn ./target/final-1.0-jar-with-dependencies.jar kafka
```

### Spark SQL
```
spark-submit --class cs523.App --master yarn ./target/final-1.0-jar-with-dependencies.jar sql
```

### Live visualization of air quality
```
python3 live_air_quality.py
```
