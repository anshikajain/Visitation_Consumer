from ConfigParser import SafeConfigParser

parser = SafeConfigParser()
parser.read('/Users/anshikajain/PycharmProjects/test/Visitation_Consumer/Config/Config.cfg')

g_atlantic_host=parser.get('atlantic', 'host')
g_cassandra_host=parser.get('cassandra', 'host')
