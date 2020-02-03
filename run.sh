export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
spark-submit --class cs523.App --master yarn ./target/final-1.0-jar-with-dependencies.jar sql keys.txt
