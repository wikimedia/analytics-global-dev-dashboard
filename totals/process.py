import MySQLdb

# wikimedia cluster information extracted from http://noc.wikimedia.org/conf/highlight.php?file=db.php
# NOTE: The default mapping is 's3'
cluster_mapping = {'enwiki':'s1','bgwiki':'s2','bgwiktionary':'s2','cswiki':'s2','enwikiquote':'s2','enwiktionary':'s2','eowiki':'s2','fiwiki':'s2','idwiki':'s2','itwiki':'s2','nlwiki':'s2','nowiki':'s2','plwiki':'s2','ptwiki':'s2','svwiki':'s2','thwiki':'s2','trwiki':'s2','zhwiki':'s2','commonswiki':'s4','dewiki':'s5','frwiki':'s6','jawiki':'s6','ruwiki':'s6','eswiki':'s7','huwiki':'s7','hewiki':'s7','ukwiki':'s7','frwiktionary':'s7','metawiki':'s7','arwiki':'s7','centralauth':'s7','cawiki':'s7','viwiki':'s7','fawiki':'s7','rowiki':'s7','kowiki':'s7'}

# new CNAME system.
# TODO: abstract mapping to a use just number and then autogenerate CNAMES aliases
db_mapping = {'s1':'s1-analytics-slave.eqiad.wmnet', 
	      's2':'s2-analytics-slave.eqiad.wmnet', 
	      's3':'s3-analytics-slave.eqiad.wmnet',
	      's4':'s4-analytics-slave.eqiad.wmnet',
	      's5':'s5-analytics-slave.eqiad.wmnet', 
	      's6':'s6-analytics-slave.eqiad.wmnet',
	      's7':'s7-analytics-slave.eqiad.wmnet', 
	      }

def get_host_name(lang):
	'''Returns the host name for the wiki project lang'''
	cluster = cluster_mapping.get('%swiki' % lang, 's3')
        return '%s-analytics-slave.eqiad.wmnet' % cluster

def main():
    languages = map(str.strip(), open('all_ids.tsv').read().split('\n'))
    ids = {}
    for lang in languages:
        host_name = get_host_name(lang)
	db = MySQLdb.connect(host=host_name,read_default_file=os.path.expanduser('~/.my.cnf'), db='%swiki' % lang)
        res = db.cursor().execute("""SELECT DISTINCT(rev_user) FROM revision""")
        ids[lang] = set(map(itemgetter(0), res))
    print ids
