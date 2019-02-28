#!/bin/bash

set echo OFF

echo "###########################################################################"
echo "#  YO!  THIS SCRIPT IS NOT PROPERLY TESTED AND DOES NOT COME WITH ANY     #"
echo "#  WARRANTIES, GUARANTEES OR PROMISES OF SUPPORT.  SERIOUSLY THINK ABOUT  #" 
echo "#  WHAT YOU'RE ABOUT TO DO, BUDDY. YOU ARE ON YOUR OWN HERE.              #"
echo "###########################################################################"
echo 

read -p "Continue (Y/n)?" choice
if [ $choice != "Y" ]; then
    exit;
fi

echo
echo "###########################################################################"
echo "#  STILL HERE?  OKAY, YOU NEED TO KNOW THIS SCRIPT MAY RESTART YOUR       #"
echo "#  CLUSTER. IF ANYONE ELSE IS WORKING ON IT ... WELL, WHO THE HELL KNOWS  #"
echo "#  WHAT MIGHT HAPPEN TO THEIR WORK.  DID I MENTION THIS SCRIPT HASN'T     #"
echo "#  BEEN FULLY TESTED?                                                          #"
echo "#                                                                         #"
echo "#  IF YOU STILL WANT TO PROCEED, ENTER THE Y KEY.                        #"
echo "###########################################################################"
echo

read -p "Continue (Y/n)?" choice2
if [ $choice2 != "Y" ]; then
    exit;
fi


# identify jar file to deploy
# ------------------------------------------------
read -ep 'Please identify the JAR to deploy as a library:  ' jar_file


# identify cluster to which to deploy
# ------------------------------------------------
echo "Please identify the cluster (by number) to which you wish to deploy this library:"
select cluster in $(databricks clusters list --output JSON | jq '.clusters[] .cluster_name');
do
    cluster_id=$(databricks clusters list --output JSON | jq '.clusters[] | select(.cluster_name | contains('$cluster'))|.cluster_id')
    echo "The \"$jar_file\" file will be deployed to cluster $cluster (cluster_id = $cluster_id)"
    
    # strip enclosing quotes
    # ----------------------------------------
    cluster_id="${cluster_id%\"}"
    cluster_id="${cluster_id#\"}"
    
    break
done


# start cluster if not already running
# ------------------------------------------------
state=$(databricks clusters list | grep $cluster_id | cut -c51-)
echo "Cluster $cluster reported in state \"$state\""
if [ "$state" = "TERMINATED" ]; then
    echo "Attempting to start cluster $cluster"
    databricks clusters start --cluster-id $cluster_id
fi


# insure landing zone for jar
# ------------------------------------------------
echo "Creating folder for jar ..."
jar_dir="myjars"
databricks fs mkdirs dbfs:/$jar_dir/$cluster_id


# uninstall library if previously loaded
# ------------------------------------------------
echo "Checking if jar already installed as library ..."
lib_jar=$(databricks libraries cluster-status --cluster-id $cluster_id | jq '.library_statuses[] .library.jar')
lib_jar="${lib_jar%\"}"
lib_jar="${lib_jar#\"}"
if [ "$lib_jar" != "" ]; then
    echo "Uninstalling the library and restarting the cluster ..."
    databricks libraries uninstall --jar $lib_jar --cluster-id $cluster_id &>/dev/null
    databricks clusters restart --cluster-id $cluster_id &>/dev/null
    sleep 30
fi


# copy jar from local fs to dbfs
# ------------------------------------------------
echo "Copying jar to dbfs ..."
databricks fs rm dbfs:/$jar_dir/$cluster_id/$jar_file
databricks fs cp $jar_file dbfs:/$jar_dir/$cluster_id/


# install library
# ------------------------------------------------
echo "Installing the jar as a library ..."
databricks libraries install --jar dbfs:/$jar_dir/$cluster_id/$jar_file --cluster-id $cluster_id
