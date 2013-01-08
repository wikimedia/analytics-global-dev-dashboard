# cd ../dashboard-data
# git rm -r --force datafiles/* datasources/* graphs/*
# mkdir datafiles datasources graphs
# cd ../dashboard

cd /home/erosen/src/dashboard/

for source in "geowiki" "grants" "mobile" "historical"
do
    for type in "dashboards" "datasources" "datafiles" "graphs"
    do
	cp $source/data/$type/* ../dashboard-data/$type
    done
done

cd ../dashboard-data
git pull
git add *
git commit -m 'automatic update using dashboard deploy_git.sh script'
git push

#ssh kripke 'sudo su www && cd /src/global-dev.wmflabs.org && git pull'
