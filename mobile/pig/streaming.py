import sys
import os
import pprint
import threading
from time import sleep
import datetime
from urlparse import urlparse, parse_qs
import urllib
import inspect
import re
import argparse
import logging

sys.path.insert(0, os.getcwd())

import subprocess
subprocess.check_call(['/bin/tar', '-xf', 'pygeoip.tar'])

import pygeoip
# import netaddr

# gi = pygeoip.GeoIP('/usr/share/GeoIP/GeoIPCity.dat')
gi = pygeoip.GeoIP('GeoIP.dat')

DATE_FORMAT = '%Y%m%d'

# logger = logging.getLogger(__name__)

class SquidRow(object):
    """ Class to represent a row from a squid file, giving access to both the literal field values
        and various derived classes.  It has a little fanciness to deal with lazily computing
        and caching infrequently-used derived fields so that it is still fast for basic file scanning.
        Specifically use the @derived_field decorator to specify that a function 'field_name' should be invoked when
        a row instance is called with  __getitem__('field_name').
    """
    #       1      2     3           4          5    6        7            8        9     10                11          12           13           14
    ids = ['host','seq','timestamp','req_time','ip','status','reply_size','method','url','squid_hierarchy','mime_type_raw','ref_header','xff_header','agent_header_raw']
    
    
    def derived_field(fn):
        """ Decorator for specifying that a function should be used to compute a derived field
            of the same name when that key is passed in to __getitem__.
        """
        args, varargs, varkw, defaults = inspect.getargspec(fn)
        if (len(args) != 1):
            raise Exception('cannot use non-unary functions with @derived_field decorator. function \'%s\' has len(args)=%d\n' % (fn.__name__, len(args))
                            + 'any function decorated as a field should take only self as the first argument\n'
                            + 'and use the object dict for any necessary information')
        fn.func_dict['derived_field'] = True
        return fn


    def __init__(self, line):
        self.__line__ = line
	toks = line.split(' ')
        if len(toks) != 14:
            #logger.debug('found row with %d space delimited tokens ' % (len(toks)))
            pass
        if len(toks) == 15:
            #logger.debug('orig toks: %s' % (toks))
            del toks[11]
            #logger.debug('new toks: %s' % (toks))
        if len(toks) < 14:
            # logger.debug('malformed line disovered:\n\t%s', line)
            raise ValueError
        self.__row__ = dict(zip(SquidRow.ids, toks))
        #logger.debug('self.__row__: %s' % (self.__row__))


    def __getitem__(self, key):
        """ implements a simple lazy evaluation and caching scheme for derived_fields: if the item 
            key is not present in the __row__ dict, look for and attribute with that name and populate
            the __row__ dict with the result of calling that attribute and return the result.  
            This scheme checks that the derived_field uses the derived_field decorator to make sure
            that only the intended functions are used.
        """
        if key in self.__row__:
            return self.__row__[key]
        elif key not in self.__row__ and hasattr(self,key) and getattr(self,key).func_dict.get('derived_field', False):
            val = getattr(self,key)()
            self.__row__[key] = val
            return val
        else:
            raise KeyError(key)


    def __setitem__(self, key, value):
        self.__row__[key] = value


    def __repr__(self):
        return 'line: %s\n__row__:%s' %  (self.__line__, pprint.pformat(self.__row__))


    """ derived field functions called by __getitem__. must use @derived_field decorator"""

    @derived_field
    def agent_header(self):
        return urllib.unquote(self.__row__['agent_header_raw']) if 'agent_header_raw' in self.__row__ else ''

    @derived_field
    def mime_type(self):
        if self['mime_type_raw'] and self['mime_type_raw'][-1] == ';':
            return self['mime_type_raw'][:-1]
        else:
            return self['mime_type_raw']

    @derived_field
    def action(self):
        return self['url_args'].get('action', [None])[0]

    @derived_field
    def url_args(self):
        return parse_qs(self['url_parsed'].query)

    @derived_field
    def xff_parsed(self):
        return urlparse(self['xff_header'])

    @derived_field
    def xff_args(self):
        return parse_qs(self['xff_parsed'])

    @derived_field
    def url_parsed(self):
        return urlparse(self['url'])

    @derived_field
    def netloc(self):
        return self['url_parsed'].netloc.split('.')

    @derived_field
    def netloc_parsed(self):
        lang_ids = ['en', 'de', 'fr', 'nl', 'it', 'pl', 'es', 'ru', 'ja', 'pt', 'sv', 'zh', 'uk', 'ca', 'no', 'fi', 'cs', 'hu', 'tr', 'ro', 'ko', 'vi', 'da', 'ar', 'eo', 'sr', 'id', 'lt', 'vo', 'sk', 'he', 'fa', 'bg', 'sl', 'eu', 'war', 'lmo', 'et', 'hr', 'new', 'te', 'nn', 'th', 'gl', 'el', 'ceb', 'simple', 'ms', 'ht', 'bs', 'bpy', 'lb', 'ka', 'is', 'sq', 'la', 'br', 'hi', 'az', 'bn', 'mk', 'mr', 'sh', 'tl', 'cy', 'io', 'pms', 'lv', 'ta', 'su', 'oc', 'jv', 'nap', 'nds', 'scn', 'be', 'ast', 'ku', 'wa', 'af', 'be-x-old', 'an', 'ksh', 'szl', 'fy', 'frr', 'yue', 'ur', 'ia', 'ga', 'yi', 'sw', 'als', 'hy', 'am', 'roa-rup', 'map-bms', 'bh', 'co', 'cv', 'dv', 'nds-nl', 'fo', 'fur', 'glk', 'gu', 'ilo', 'kn', 'pam', 'csb', 'kk', 'km', 'lij', 'li', 'ml', 'gv', 'mi', 'mt', 'nah', 'ne', 'nrm', 'se', 'nov', 'qu', 'os', 'pi', 'pag', 'ps', 'pdc', 'rm', 'bat-smg', 'sa', 'gd', 'sco', 'sc', 'si', 'tg', 'roa-tara', 'tt', 'to', 'tk', 'hsb', 'uz', 'vec', 'fiu-vro', 'wuu', 'vls', 'yo', 'diq', 'zh-min-nan', 'zh-classical', 'frp', 'lad', 'bar', 'bcl', 'kw', 'mn', 'haw', 'ang', 'ln', 'ie', 'wo', 'tpi', 'ty', 'crh', 'jbo', 'ay', 'zea', 'eml', 'ky', 'ig', 'or', 'mg', 'cbk-zam', 'kg', 'arc', 'rmy', 'gn', '(closed)', 'so', 'kab', 'ks', 'stq', 'ce', 'udm', 'mzn', 'pap', 'cu', 'sah', 'tet', 'sd', 'lo', 'ba', 'pnb', 'iu', 'na', 'got', 'bo', 'dsb', 'chr', 'cdo', 'hak', 'om', 'my', 'sm', 'ee', 'pcd', 'ug', 'as', 'ti', 'av', 'bm', 'zu', 'pnt', 'nv', 'cr', 'pih', 'ss', 've', 'bi', 'rw', 'ch', 'arz', 'xh', 'kl', 'ik', 'bug', 'dz', 'ts', 'tn', 'kv', 'tum', 'xal', 'st', 'tw', 'bxr', 'ak', 'ab', 'ny', 'fj', 'lbe', 'ki', 'za', 'ff', 'lg', 'sn', 'ha', 'sg', 'ii', 'cho', 'rn', 'mh', 'chy', 'ng', 'kj', 'ho', 'mus', 'kr', 'hz', 'mwl', 'pa', 'xmf', 'lez']
        site_ids = {'m' : 'M', 'zero' : 'Z'}
        netloc = self['netloc']

        # get lang if present
        if netloc[0] in lang_ids:
            lang = netloc[0]
            netloc = netloc[1:]
        else:
            lang = None

        # get site if present
        if netloc[0] in site_ids:
            site = site_ids[netloc[0]]
            netloc = netloc[1:]
        else:
            site = 'X'

        # get project
        project = '.'.join(netloc[:-1])
        return {'project' : project, 'site' : site, 'lang' : lang}

    @derived_field
    def site(self):
        return self['netloc_parsed']['site']

    @derived_field
    def lang(self):
        return self['netloc_parsed']['lang']

    @derived_field
    def project(self):
        return self['netloc_parsed']['project']

    @derived_field
    def bot(self):
        bot_pat = ".*([Bb][Oo][Tt])|([Cc]rawl(er)?)|([Ss]pider)|(http://).*"
        return bool(re.match(bot_pat, self['agent_header']))

    @derived_field
    def url_path(self):
        return self['url_parsed'].path.split('/')[1:]

    @derived_field
    def datetime(self):
        # format: 2012-07-16T06:51:29.534
        try:
            return datetime.datetime.strptime(self['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
        except ValueError:
            # sometimes there are no seconds ...
            return datetime.datetime.strptime(self['timestamp'], '%Y-%m-%dT%H:%M:%S')

    @derived_field
    def date(self):
        return str(self['datetime'].date())

    @derived_field
    def title(self):
        path = self['url_path']
        if path[0] != 'wiki':
            return None
        return path[-1]

    @derived_field
    def device_type(self):
        """
        this function uses regexes found in Erik Zachte and Andre Engels code:
        
           https://svn.wikimedia.org/viewvc/mediawiki/trunk/wikistats/squids/SquidCountArchiveProcessLogRecord.pm?view=markup

        (with some minor perl-to-python modifications) to tag user agent strings with device type.
        When a string is tagged as multiple categories i user a handwritten ordering of tag precedence
        in attempt to compensate for the broad nature of the mobile category
        """
        tags = {}
        tags['-'] = '^-$'
        tags['wiki_mobile'] = "CFNetwork|Dalvik|WikipediaMobile|Appcelerator|WiktionaryMobile|Wikipedia Mobile"
        tags['mobile'] = "Android|BlackBerry|Windows CE|DoCoMo|iPad|iPod|iPhone|HipTop|Kindle|LGE|Linux arm|MIDP|NetFront|Nintendo|Nokia|Obigo|Opera Mini|Opera Mobi|Palm|Playstation|Samsung|SoftBank|SonyEricsson|Symbian|UP\.Browser|Vodafone|WAP|webOS|HTC[^P]|KDDI|FOMA|Polaris|Teleca|Silk|ZuneWP|HUAwei|Sunrise XP|Sunrise/|AUDIOVOX|LG/U|AU-MIC|Motorola|portalmmm|Amoi|GINGERBREAD|Spice|lgtelecom|PlayBook|KYOCERA|Opera Tablet|Windows Phone|UNTRUSTED|Sensation|UCWEB|Nook|XV6975|EBRD1|Rhodium|UPG|Symbian|Pantech|MeeGo|Tizen"
        tags['tablet'] = "iPad|Android 3|SCH-I800|Kindle Fire|Xoom|GT-P|Transformer|SC-01C|pandigital|SPH-P|STM803HC|K080|SGH-T849|CatNova|NookColor|M803HC|A1_|SGH-I987|Ideos S7|SHW-M180|HomeManager|HTC_Flyer|PlayBook|Streak|Kobo Touch|LG-V905R|MID7010|CT704|Silk|MID7024|ARCHM|Iconia|TT101|CT1002|; A510|MID_Serials|ZiiO10|MID7015|001DL|MID Build|PM1152|RBK-490|Tablet|A100 Build|ViewPad|PMP3084|PG41200|; A500|A7EB|A80KSC"
        matches = {}
        for device_type, tags in tags.items():
            match = re.match('.*(?:%s).*' % (tags), self['agent_header'])
            if match:
                matches[device_type] = match
        # use inverse popularity as precedence ordering
        #logger.debug('matches: %s' % (matches.keys()))
        if 'tablet' in matches:
            return 'tablet'
        if 'wiki_mobile' in matches:
            return 'wiki_mobile'
        if 'mobile' in matches:
            return 'mobile'
        if '-' in matches:
            return '-'
        else:
            #logger.debug('no matches found. ua: %s' % (ua))
            return 'unknown'

    @derived_field
    def status_code(self):
        if '/' in self['status']:
            status_str = self['status'].split('/')[1]
        else:
            # logger.debug('found non-backslash delimited status field: %s', self['status'])
            status_str = self['status']
        try:
            status = int(status_str)
        except ValueError:
            # logger.warning('could not decode status: %s', self['status'])
            status = None
        return status

    @derived_field
    def init_request(self):
        return self['mime_type'] == 'text/html' and self['status_code'] < 300 and self['url_path'] and self['url_path'][0] == 'wiki'

    @derived_field
    def old_init_request(self):
        return self['url_path'] and self['url_path'][0] == 'wiki'

    @derived_field
    def country(self):
        try:
            code = gi.country_code_by_addr(self['ip'])
            # logger.debug('parsed ip: %s as originating from: %s',  self['ip'], code)
            return code
        except:
            #logger.exception('error retrieving info for ip: %s', self['ip'])
             pass
        return None

    # partner_ip_ranges = {'digi-malaysia' : ['203.92.128.185',
    #                                         '115.164.0.0/16', 
    #                                         '116.197.0.0/17'],
    #                      'digi-malaysia-opera' : ['195.189.142.0/23',
    #                                               '91.203.96.0/22',
    #                                               '80.239.242.0/23',
    #                                               '217.212.230.0/23',
    #                                               '141.0.8.0/21',
    #                                               '2001:4c28:194::/48', 
    #                                               '82.145.208.0/20',
    #                                               '2001:4c28:1::/48'],
    #                      'orange-ivory-coast' : ['41.66.28.94',
    #                                              '41.66.28.95',
    #                                              '41.66.28.96',
    #                                              '41.66.28.72',
    #                                              '41.66.28.73'],
    #                      'orange-uganda' : ['41.202.224.0/19',
    #                                         '197.157.0.0/18'],
    #                      'orange-tunesia' : ['197.30.0.0/17', 
    #                                          '197.30.128.0/19',
    #                                          '197.30.224.0/19',
    #                                          '197.29.192.0/18'],
    #                      'telenor-montenegro' : ['79.143.96.0/20'],
    #                      'orange-niger' : ['41.203.157.1',
    #                                        '41.203.157.2',
    #                                        '41.203.159.243'],
    #                      'orange-cameroon' : ['193.251.155.0/25',
    #                                           '41.202.192.0/19',
    #                                           '41.202.219.64/28',
    #                                           '41.202.219.112/30',
    #                                           '41.202.219.14',
    #                                           '41.202.219.10'],
    #                      'orange-kenya' : ['212.49.88.0/25'],
    #                      'tata-gsm' : ['182.156.0.0 /16',
    #                                    '27.107.0.0 /16',
    #                                    '14.194.0.0 /16',
    #                                    '14.195.0.0 /16',
    #                                    '49.200.0.0 /16',
    #                                    '49.201.0.0 /16',
    #                                    '49.202.0.0 /16',
    #                                    '49.203.0.0 /16',
    #                                    '49.249.64.0/18',
    #                                    '49.249.128.0/18',
    #                                    '49.249.192.0/19',
    #                                    '49.248.224.0/20',
    #                                    '49.248.240.0/22',
    #                                    '49.248.244.0/23',
    #                                    '49.249.0.0/19',
    #                                    '49.249.32.0/19',
    #                                    '49.249.224.0/19',
    #                                    '115.117.192.0/18',
    #                                    '115.118.208.0/20',
    #                                    '115.118.224.0/19',
    #                                    '115.118.16.0/22',
    #                                    '115.118.44.0/22',
    #                                    '115.118.48.0/20',
    #                                    '115.118.80.0/20',
    #                                    '115.118.108.0/22',
    #                                    '115.118.144.0/20',
    #                                    '115.118.180.0/21',
    #                                    '115.118.188.0/22',
    #                                    '115.118.204.0/22',
    #                                    '115.117.128.0/18'],
    #                      'tata-dive-in-delhi' : ['59.161.254.21'],
    #                      'tata-dive-in-mumbai' : ['182.156.191.10'],
    #                      'tata-cdma' : ['219.64.175.132',
    #                                     '219.64.175.134',
    #                                     '219.64.175.135',
    #                                     '219.64.175.136',
    #                                     '219.64.175.137',
    #                                     '219.64.175.139',
    #                                     '219.64.175.142'],
    #                      'tata-brew-public-ips' : ['59.161.95.86',
    #                                                '219.64.175.215',
    #                                                '219.64.175.216',
    #                                                '219.64.175.217',
    #                                                '219.64.175.219',
    #                                                '14.96.246.43',
    #                                                '14.96.246.36',
    #                                                '14.96.246.40',
    #                                                '14.96.246.41',
    #                                                '14.96.246.42',
    #                                                '59.161.95.65',
    #                                                '59.161.95.66',
    #                                                '59.161.95.67',
    #                                                '59.161.95.68'],
    #                      'vodafone-india' : ['203.88.0.0/19',
    #                                          '114.31.128.0/18',
    #                                          '112.79.0.0/16',
    #                                          '1.38.0.0/15',
    #                                          '42.104.0.0/13'],
    #                      'saudi-telecom 212.118.140.0/22' : ['212.118.140.0/22'],
    #                      'saudi-telecom 212.215.128.0/17' : ['212.215.128.0/17'],
    #                      'saudi-telecom 84.235.72.0/22'   : ['84.235.72.0/22'],
    #                      'saudi-telecom 84.235.94.240/28' : ['84.235.94.240/28'],
    #                      # 'saudi-telecom' : ['212.118.140.0/22',
    #                      #                    '212.215.128.0/17',
    #                      #                    '84.235.72.0/22',
    #                      #                    '84.235.94.240/28'],
    #                      'grameenphone-bangladesh' : ['119.30.38.0/24',
    #                                                   '119.30.39.0/24',
    #                                                   '119.30.45.0/24',
    #                                                   '119.30.47.0/24'],
    #                      'grameenphone-bangladesh-opera-mini' : ['91.203.96.103',
    #                                                              '195.189.142.0/23',
    #                                                              '91.203.96.0/22',
    #                                                              '80.239.242.0/23',
    #                                                              '217.212.230.0/23',
    #                                                              '141.0.8.0/21',
    #                                                              '2001:4c28:194::/48',
    #                                                              '82.145.208.0/20',
    #                                                              '2001:4c28:1::/48'],
    #                      'dtac-thailand' : ['115.67.0.0/16',
    #                                         '111.84.0.0/16',
    #                                         '1.46.0.0/16',
    #                                         '1.47.0.0/16',
    #                                         '103.1.164.0/22',
    #                                         '202.91.16.0/21'],
    #                      'dtac-thailand-opera-mini' : ['80.239.242.0/23',
    #                                                   '82.145.208.0/20',
    #                                                   '91.203.96.0/22',
    #                                                   '141.0.8.0/21',
    #                                                   '195.189.142.0/23',
    #                                                   '217.212.230.0/23',
    #                                                   '217.212.226.0/24',
    #                                                   '37.228.104.0/21'],
    #                      'dialog-sri-lanka' : ['175.157.0.0 /16',
    #                                            '111.223.128.0/18',
    #                                            '103.2.148.0/22',
    #                                            '123.231.8.0/21',
    #                                            '123.231.40.0/21',
    #                                            '123.231.48.0/21',
    #                                            '123.231.56.0/21',
    #                                            '123.231.120.0/21',
    #                                            '182.161.0.0/19',
    #                                            '122.255.52.0/24',
    #                                            '122.255.53.0/24',
    #                                            '122.255.54.0/24'],
    #                      'hello-cambodia' : ['117.20.116.83',
    #                                          '117.20.116.84/30',
    #                                          '117.20.116.88/30',
    #                                          '117.20.116.92/31'],
    #                      'celcom-malaysia' : ['203.82.80.0/24',
    #                                           '203.82.81.0/24',
    #                                           '203.82.82.0/24',
    #                                           '203.82.87.0/24',
    #                                           '183.171.64.0/18',
    #                                           '203.82.95.0/24',
    #                                           '183.171.128.0/19',
    #                                           '203.82.90.0/24',
    #                                           '203.82.91.0/24',
    #                                           '203.82.92.0/24',
    #                                           '203.82.93.0/24',
    #                                           '203.82.94.0/24',
    #                                           '183.171.0.0/18'],
    #                      'orange-congo' : ['91.151.146.64/27',
    #                                        '203.222.206.112/28',
    #                                        '203.222.195.80/28',
    #                                        '81.199.62.88/29',
    #                                        '81.199.62.128/26'],
    #                      'orange-botswana' : ['41.223.141.160/29',
    #                                           '41.223.141.80/29',
    #                                           '41.223.141.81/29',
    #                                           '41.223.141.82/29',
    #                                           '41.223.141.83/29',
    #                                           '41.223.141.84/29',
    #                                           '41.223.141.85/29',
    #                                           '41.223.141.86/2',
    #                                           '41.223.142.92/32']
    #                      }

    # @derived_field
    # def provider(self):
    #     try:
    #         ip = netaddr.IPAddress(self['ip'])
    #         for provider, ranges in SquidRow.partner_ip_ranges.items():
    #             for cidr_range in ranges:
    #                 subnet = netaddr.IPNetwork(cidr_range)
    #                 prefix_len = subnet.prefixlen
    #                 if ip.bits()[:prefix_len] == subnet.ip.bits()[:prefix_len]:
    #                     return provider
    #         return None
    #     except:
    #         # logger.exception('error identifying partner for ip: %s', self['ip'])
    #         return None


FIELDS = ['date', 'lang', 'project', 'site', 'country']

default = '\t'.join(['','','','','',''])

def parse_line(line):
    try:
	toks = line.split()
	filename = toks[0]
	rest = ' '.join(toks[1:])

	row = SquidRow(rest)
        if not row['old_init_request']:
            return None
        return '\t'.join(map(str, [filename] + map(row.__getitem__, FIELDS))) + '\n'
    except:
        return None


if True:#if __name__=='__main__':
    for line in sys.stdin:
        parsed = parse_line(line)
        if parsed is not None:
            sys.stdout.write(parse_line(line))
	    sys.stdout.flush()
