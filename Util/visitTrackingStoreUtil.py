import subprocess
from threading import Timer
import unittest
import sys
import os
import time
from Config.configParser import *
from cassandra.cluster import Cluster


class visitTrackingStoreUtil:
    def trigger_events(self, event):

        # Triggering Ad Exposure Kafka Events
        HOST = g_atlantic_host
        COMMAND = 'curl -X POST --data ' + '"' + event + '" ' + HOST + ':8086/discoveryProducerApp/test/start'
        ssh = subprocess.Popen(["ssh -o StrictHostKeyChecking=no", "%s" % HOST, COMMAND],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        result = ssh.stdout.readlines()
        if result == []:
            error = ssh.stderr.readlines()
            print >> sys.stderr, "NO OUTPUT FROM STDOUT: %s" % error
        else:
            print "Visit Tracking Kafka Event is:", result
        ssh.kill()
        assert result[0].startswith('Succeed'), "Test case failed while sending Kafka Events"
        time.sleep(20)

    def validate_from_cassandra(self, adgroup):

        # Connecting to Cassandra
        HOST_CASSANDRA = g_cassandra_host
        KEYSPACE = "visitation_consumer"
        cluster = Cluster([HOST_CASSANDRA])
        session = cluster.connect()
        session.execute("use visitation_consumer")

        # Getting Adgroup id from cassandra
        adgroup = "select * from visit_tracking_store where adgroup =" + adgroup
        rows = session.execute(adgroup)
        print "My row for Adgroup is", rows
        assert rows != [], "Test case failed while getting Adgroup from Cassandra Visit Tracking Store table"

    def norows_3para_from_cassandra(self, adgroup, timestamp, requestid):

        # Connecting to Cassandra
        HOST_CASSANDRA = g_cassandra_host
        KEYSPACE = "visitation_consumer"
        cluster = Cluster([HOST_CASSANDRA])
        session = cluster.connect()
        session.execute("use visitation_consumer")

        # Getting Adgroup id from cassandra
        adgroup = "select * from visit_tracking_store where adgroup =" + adgroup + " and timestamp=" + timestamp + " and request_id=" + "'" + requestid + "'"
        print "My adgroup is:",adgroup
        rows = session.execute(adgroup)
        print "My row for Adgroup is", rows
        assert rows == [], "Test case failed while getting Adgroup and Timestamp and RequestId from Cassandra Visit Tracking Store table"

    def norows_from_cassandra(self, adgroup):

        # Connecting to Cassandra
        HOST_CASSANDRA = g_cassandra_host
        KEYSPACE = "visitation_consumer"
        cluster = Cluster([HOST_CASSANDRA])
        session = cluster.connect()
        session.execute("use visitation_consumer")

        # Getting Adgroup from cassandra
        adgroup = "select * from visit_tracking_store where adgroup =" + adgroup
        rows = session.execute(adgroup)
        print "My row for Adgroup is", rows
        assert rows == [], "Test case failed while getting Adgroup from Cassandra Visit Tracking Store table"


    def delete_from_cassandra(self,adgroup,cookieid,deviceid,ip):

        # Connecting to Cassandra
        HOST_CASSANDRA = g_cassandra_host
        KEYSPACE = "visitation_consumer"
        cluster = Cluster([HOST_CASSANDRA])
        session = cluster.connect()
        session.execute("use visitation_consumer")

        # Delete Adgroup,CookieId,DeviceId,IP from Cassandra tables

        if adgroup != '':
            rows = "delete from visit_tracking_store where adgroup =" + adgroup
            rows = session.execute(rows)

        if cookieid != '':
            rows = "delete from ad_exposure_cookie_id_state where cookie_id =" + "'" + cookieid + "'"
            rows = session.execute(rows)

        if deviceid != '':
            rows = "delete from ad_exposure_device_id_state where device_id =" + "'" + deviceid + "'"
            rows = session.execute(rows)

        if ip != '':
            rows = "delete from ad_exposure_ip_state where ip=" + "'" + ip + "'"
            rows = session.execute(rows)

        print "Successfully Deleted the Adgroup,Cookie Id,Device Id,IP from tables"
