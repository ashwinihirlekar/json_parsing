#!/bin/sh
########################################################
#Developed by Ashwini Hirlekar on 24 Sep 2017
#This script converts input json file into flat file   #
# with required attributes                             #
#Parameters                                            #
# $1 - Queue name                                      #
# $2 - input directory                                 #
# $3 - output directory                                #
########################################################

ts=$(date +'%Y%m%d')
mail_list=abc@gmail.com
filename=$(basename $2)
feed_key=`cut -d'_' -f3 <<< $filename | cut -c-10`
julian_date=`date -d $feed_key +%Y%j`

echo "julian_date " $julian_date

echo "Script execution has started at $(date +'%Y-%m-%d %H:%M:%S')"|tee -a jsonParser_$ts".log"

echo "Executing Python MR"|tee -a jsonParser_$ts".log"

hadoop2 jar /opt/mapr/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar -Dmapred.job.queue.name=$1 -D mapred.reduce.tasks=0 -input $2 -output $3 -mapper "/usr/bin/python jsonParserMapper.py" -file /json_parser_demo/pymr/change/jsonParserMapper.py >> jsonParser_$ts".log" 2>&1

if [ $? -eq 0 ]; then
        echo "Conversion completed and output available at $3" |  mailx -s "JSON parser script completed successfully on `date +'%Y-%m-%d-%H:%M:%S'`" $mail_list
else
	echo "The JSON parser script has failed for RC response"| mailx -s "JSON parser script failure for RC response on `date +'%Y-%m-%d-%H:%M:%S'`" $mail_list
        exit 1;
fi

echo "Executing Hadoop getmerge"|tee -a jsonParser_$ts".log"

hadoop fs -getmerge   /inter_output/*   /output/rc_rsponse_D"$julian_date" 
if [ $? -eq 0 ]; then
        echo "Merging part files completed" | mailx -s "Merging part files completed successfully and file is ready for ingestion on `date +'%Y-%m-%d-%H:%M:%S'`" $mail_list
	echo "Deleting output directory - $3" |tee -a jsonParser_$ts".log"
	rm -r $3
else
        echo "Merging part files failed"| mailx -s "Merging part files failure for RC response on `date +'%Y-%m-%d-%H:%M:%S'`" $mail_list
        exit 1;
fi
echo "Script execution has ended at $(date +'%Y-%m-%d %H:%M:%S')"|tee -a jsonParser_$ts".log"

