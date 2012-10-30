cd ../dashboard-data
git rm -r --force datafiles/* datasources/* graphs/*
mkdir datafiles datasources graphs

cd ../dashboard

for source in "geowiki" "grants" "mobile" "historical"
do
    for type in "datasources" "datafiles" "graphs"
    do
	cp $source/$type/* ../dashboard-data/$type
    done
done

cd ../dashboard-data
git add *
git commit -m 'automatic update using dashboard deploy_git.sh script'
git push

#ssh kripke 'sudo su www && cd /src/global-dev.wmflabs.org && git pull'
