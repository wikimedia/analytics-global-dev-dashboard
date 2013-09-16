# cd ../dashboard-data
# git rm -r --force datafiles/* datasources/* graphs/*
# mkdir datafiles datasources graphs
# cd ../dashboard

cd /home/erosen/src/dashboard/

cd ../dashboard-data
git pull
cd ../dashboard

for source in "geowiki" "grants" "historical" "db_size"
do
    for type in "dashboards" "datasources" "datafiles" "graphs" "geo"
    do
        echo "executing: cp -r $source/data/$type/* ../dashboard-data/$type"
        cp -r $source/data/$type/* ../dashboard-data/$type
    done
done

cd ../dashboard-data
git add *
git commit -a -m 'automatic update using dashboard deploy_git.sh script'
git push

#ssh kripke 'sudo su www && cd /src/global-dev.wmflabs.org && git pull'
