import os
from ConfigParser import SafeConfigParser

parser = SafeConfigParser()
# locate Excel sheet
conf_dir = os.path.dirname(os.path.realpath(__file__))
conf_dir = conf_dir + '/../Config.cfg'
parser.read(conf_dir)

g_atlantic_host=parser.get('atlantic', 'host')
g_cassandra_host=parser.get('cassandra', 'host')
