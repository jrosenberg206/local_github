#!/bin/bash
echo "Exporting DB connections"
export bio_password=Cellars4567
export bio_username=craig@aandkshellfish.com
export bio_ubi=603355124
export dbuser=dbmaster
export dbpassword=SqueezeTheData
export dbhost=taste01.coqbvn09pir1.us-west-2.rds.amazonaws.com
export dbport=3306

python /data/api/shc-3.8.7/ScheduledLED2.py API

sleep 60

mysql -htaste01.coqbvn09pir1.us-west-2.rds.amazonaws.com -udbmaster -pSqueezeTheData rootcellar < /data/api/shc-3.8.7/DailyC_Mysql.txt

