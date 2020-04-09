# Final Project of CS523 - Big Data Technology

## Professor: Mrudula Mukadam
## Team members of Group 1
* Tam Van Vo - 610746
* Minh Tuan Bui - 610582
* Bao Nguyen Nguyen - 610116

## Slide Presentation at [here](Presentation-Observing_the_Air_Quality.pptx) or [OneDrive](https://onedrive.live.com/view.aspx?resid=35EA824A9571A24D!418)

## Getting Started

### Install Spark 2.4.4 on Hadoop 2.6
```
sh setup_spark2.sh
```

### Setting enviroment $HADOOP_CONF_DIR
```
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
```
### AWS Lambda function triggered by SNS topic

```
deploy lambda.py to AWS and add SNS trigger arn:aws:sns:us-east-1:470049585876:OPENAQ_NEW_MEASUREMENT
```
:warning: it will be triggered every minute and create many S3 files in bucket air-quality-live.

### Publish data to Kafka
```
java -cp ./target/final-1.0-jar-with-dependencies.jar cs523.agent.App US pm25 2020-01-01T00:00:00 2020-02-01T00:00:00
```

### Spark streaming from S3 bucket s3://air-quality-live
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
