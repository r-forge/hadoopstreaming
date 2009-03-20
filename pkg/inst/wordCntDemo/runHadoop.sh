#!/bin/env bash
HADOOP="$HADOOP_HOME/bin/hadoop"   # Hadoop command
HADOOPSTREAMING="$HADOOP jar $HADOOP_HOME/contrib/streaming/hadoop-0.19.1-streaming.jar" # change version number as appropriate
RLIBPATH="~/RLibrary"   # can specify additional R Library paths here

INPUTFILE="anna.txt"
INPUTDIR="input"
OUTDIR="annaWordCnt"
RFILE="hsWordCnt.R"
LOCALOUT="annaWordCnts.out"
# Put the file into the Hadoop file system
$HADOOP fs -put $INPUTFILE $INPUTDIR

# Remove the directory if already exists (otherwise, won't run)
$HADOOP fs -rmr $OUTDIR

MAPARGS="--mapper"  
REDARGS="--reducer"

$HADOOPSTREAMING -input $INPUTDIR/$INPUTFILE -output $OUTDIR -mapper "$RFILE $MAPARGS" -reducer "$RFILE $REDARGS" -file $RFILE -cmdenv R_LIBS=$RLIBPATH 

# Extract output
./$RFILE --reducecols > $LOCALOUT
$HADOOP fs -cat $OUTDIR/part* >> $LOCALOUT


# Look at output directory
# $HADOOP fs -ls $OUTDIR

# Look at output
#$HADOOP fs -cat $OUTDIR/part*

# Look at error log 
# $HADOOP fs -cat $OUTDIR/_logs/history/*.jar


