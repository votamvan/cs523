sudo yum remove -y java
sudo yum install -y java-1.8.0-openjdk-devel
tar xzvf spark-2.4.4-bin-hadoop2.6.tgz
mv spark-2.4.4-bin-hadoop2.6 /usr/local/spark

# edit /usr/bin/pyspark
sudo cat > /usr/bin/pyspark << ENDOFFILE
#!/bin/bash

# Autodetect JAVA_HOME if not defined
. /usr/lib/bigtop-utils/bigtop-detect-javahome

export PYSPARK_PYTHON=${PYSPARK_PYTHON:-python}

exec /usr/local/spark/bin/pyspark "$@"
ENDOFFILE

# edit /usr/bin/spark-submit
sudo cat > /usr/bin/spark-submit << ENDOFFILE
#!/bin/bash

# Autodetect JAVA_HOME if not defined
. /usr/lib/bigtop-utils/bigtop-detect-javahome

exec /usr/local/spark/bin/spark-submit "$@"
ENDOFFILE

# edit /usr/bin/spark-shell
sudo cat > /usr/bin/spark-shell << ENDOFFILE
#!/bin/bash

# Autodetect JAVA_HOME if not defined
. /usr/lib/bigtop-utils/bigtop-detect-javahome

exec /usr/local/spark/bin/spark-shell "$@"
ENDOFFILE

