LOGS = LOAD '$input' USING PigStorage(' ', '-tagsource') AS (
    path,
    hostname,
    udplog_sequence,
    timestamp:chararray,
    request_time,
    remote_addr:chararray,
    http_status,
    bytes_sent,
    request_method:chararray,
    uri:chararray,
    proxy_host,
    content_type:chararray,
    referer,
    x_forwarded_for,
    user_agent);

--STORE LOGS INTO 'logs_with_fname' USING PigStorage('\t');

DEFINE ParseUrl `python streaming.py` SHIP('streaming.py', 'GeoIP.dat', 'pygeoip.tar');
--DEFINE ParseUrl `python min.py` SHIP('min.py', 'evan_test'); 
PARSED = STREAM LOGS THROUGH ParseUrl AS (
			filename:chararray,
			date:chararray, 
			lang:chararray, 
			project:chararray, 
			site:chararray, 
			country:chararray);
GROUPED = GROUP PARSED BY (filename, date, lang, project, site, country);
COUNTS = FOREACH GROUPED GENERATE FLATTEN(group), COUNT_STAR(PARSED);
STORE COUNTS INTO '$output' USING PigStorage(',');
