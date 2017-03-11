import subprocess
from threading import Timer
import unittest
import sys
import os
import time
from Config.configParser import *
from cassandra.cluster import Cluster

class adExposureEventsUtil:



    def trigger_events(self,event):

        # Triggering Ad Exposure Kafka Events
        HOST = g_atlantic_host
        COMMAND = 'curl -X POST --data ' + '"' + event + '" ' + HOST + ':8086/visitationProducer/test/start'
        ssh = subprocess.Popen(["ssh", "%s" % HOST, COMMAND],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        result = ssh.stdout.readlines()
        if result == []:
            error = ssh.stderr.readlines()
            print >> sys.stderr, "NO OUTPUT FROM STDOUT: %s" % error
        else:
            print "Ad Exposure Kafka Event is:", result
        ssh.kill()

        assert result[0].startswith('Succeed'), "Test case failed while sending Kafka Events"
        time.sleep(20)

    def not_trigger_events(self,event):

        # Triggering Ad Exposure Kafka Events
        HOST = g_atlantic_host
        COMMAND = 'curl -X POST --data ' + '"' + event + '" ' + HOST + ':8086/visitationProducer/test/start'
        ssh = subprocess.Popen(["ssh", "%s" % HOST, COMMAND],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        result = ssh.stdout.readlines()
        if result == []:
            error = ssh.stderr.readlines()
            print >> sys.stderr, "NO OUTPUT FROM STDOUT: %s" % error
        else:
            print "Ad Exposure Kafka Event is:", result
        ssh.kill()

        assert result[0].startswith('Oops'), "Test case failed while sending Kafka Events"
        time.sleep(20)

    def validate_from_cassandra(self,cookieid,deviceid,ip):
        
        # Connecting to Cassandra
        HOST_CASSANDRA = g_cassandra_host
        KEYSPACE = "visitation_consumer"
        cluster = Cluster([HOST_CASSANDRA])
        session = cluster.connect()
        session.execute("use visitation_consumer")

        # Getting Cookie id from cassandra
        if cookieid != '':
            cookie_id = "select * from ad_exposure_cookie_id_state where cookie_id =" + "'" + cookieid + "'"
            rows = session.execute(cookie_id)
            print "My row for Cookie Id is", rows
            assert rows != [], "Test case failed while getting cookie id from Cassandra"

        # Getting Device Id from cassandra
        if deviceid != '':
            device_id =  "select * from ad_exposure_device_id_state where device_id =" + "'" + deviceid + "'"
            rows = session.execute(device_id)
            print "My row for Device Id is", rows
            assert rows != [], "Test case failed while getting device id from Cassandra"

        # Getting IP from cassandra
        if ip != '':
            myip = "select * from ad_exposure_ip_state where ip=" + "'" + ip + "'"
            rows = session.execute(myip)
            print "My row for IP is", rows
            assert rows != [], "Test case failed while getting ip from Cassandra"

        # Delete DeviceId,CookieId,IP from Cassandra tables
        if cookieid != '':
            rows = "delete from ad_exposure_cookie_id_state where cookie_id =" + "'" + cookieid + "'"
            rows = session.execute(rows)

        if deviceid != '':
            rows = "delete from ad_exposure_device_id_state where device_id =" + "'" + deviceid + "'"
            rows = session.execute(rows)

        if ip != '':
            rows = "delete from ad_exposure_ip_state where ip=" + "'" + ip + "'"
            rows = session.execute(rows)

        print "Successfully Deleted the Cookie Id,Device Id,IP from tables"

    def norows_from_cassandra(self, cookieid, deviceid, ip):

        # Connecting to Cassandra
        HOST_CASSANDRA = g_cassandra_host
        KEYSPACE = "visitation_consumer"
        cluster = Cluster([HOST_CASSANDRA])
        session = cluster.connect()
        session.execute("use visitation_consumer")

        # Getting Cookie id from cassandra
        cookie_id = "select * from ad_exposure_cookie_id_state where cookie_id =" + "'" + cookieid + "'"
        rows = session.execute(cookie_id)
        print "My row for Cookie Id is", rows
        assert rows == [], "Test case failed while getting cookie id from Cassandra"

        # Getting Device Id from cassandra
        device_id = "select * from ad_exposure_device_id_state where device_id =" + "'" + deviceid + "'"
        rows = session.execute(device_id)
        print "My row for Device Id is", rows
        assert rows == [], "Test case failed while getting device id from Cassandra"

        # Getting IP from cassandra
        myip = "select * from ad_exposure_ip_state where ip=" + "'" + ip + "'"
        rows = session.execute(myip)
        print "My row for IP is", rows
        assert rows == [], "Test case failed while getting ip from Cassandra"
