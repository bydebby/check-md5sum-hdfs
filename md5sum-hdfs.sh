folder=$1
listfile="$(basename list${folder})"
listfilehdfs="${listfile}.dat"
#mkdir -p /data/backup_datalake/md5sumhdfs${folder}
#hadoop fs -ls  $folder | sed '1d;s/  */ /g' | cut -d\  -f8 > $listfilehdfs

hadoop fsck ${folder} -files | grep block | awk '{print $1}' > $listfilehdfs

for xx in `cat $listfilehdfs`; do

#mkdir -p /data/backup_datalake/md5sumhdfs${xx}

checkmd5sum=$(hadoop fs -cat ${xx}|md5sum | sed 's/-//g') #> /data/backup_datalake/md5sumhdfs${xx}/md5sum_$(basename list${xx}).dat

#echo ${xx}.dat
#find /data/backup_datalake/md5sumhdfs${folder} -type f -exec cat {} \;) #> listmd5sum/md5sum_$listfile
#find /data/backup_datalake/md5sumhdfs${folder} -type f -exec ls {} \;) #> listmd5sum/checkmd5sum_$listfile
#paste listmd5sum/md5sum_$listfile listmd5sum/checkmd5sum_$listfile | column -s $'\t' -t > listmd5sum/mergemd5sum_$listfile

merge="${checkmd5sum} ${xx}"
echo ${merge} >> md5sumhdfs_${listfile}.csv

done;
