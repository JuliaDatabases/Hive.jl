HADOOP_VER=2.7.1
export HADOOP_VER

HADOOP_PREFIX=/hadoop/hadoop-$HADOOP_VER
export HADOOP_PREFIX

HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_HOME

HADOOP_MAPRED_HOME=$HADOOP_PREFIX
export HADOOP_MAPRED_HOME

HADOOP_COMMON_HOME=$HADOOP_PREFIX
export HADOOP_COMMON_HOME

HADOOP_HDFS_HOME=$HADOOP_PREFIX
export HADOOP_HDFS_HOME

HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
export HADOOP_CONF_DIR

YARN_HOME=$HADOOP_PREFIX
export YARN_HOME

YARN_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
export YARN_CONF_DIR

PATH=$PATH:$HADOOP_PREFIX/bin:$HADOOP_PREFIX/sbin
export PATH