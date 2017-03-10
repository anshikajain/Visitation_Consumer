import subprocess
from threading import Timer
import unittest
import sys
import os
import time
from Visitation_Consumer.Config.configParser import *
from cassandra.cluster import Cluster


class pixelConversionUtil:

    def trigger_events(self, event):

        # Triggering Ad Exposure Kafka Events
        HOST = g_atlantic_host
        COMMAND = 'curl -X POST --data ' + '"' + event + '" ' + HOST + ':8086/discoveryProducerApp/test/start'
        ssh = subprocess.Popen(["ssh", "%s" % HOST, COMMAND],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        result = ssh.stdout.readlines()
        if result == []:
            error = ssh.stderr.readlines()
            print >> sys.stderr, "NO OUTPUT FROM STDOUT: %s" % error
        else:
            print "Pixel Conversion Event is:", result
        ssh.kill()
        assert result[0].startswith('Succeed'), "Test case failed while sending Kafka Events"
        time.sleep(20)

    def validate_from_cassandra(self, adgroup, pixelid):

        # Connecting to Cassandra
        HOST_CASSANDRA = g_cassandra_host
        KEYSPACE = "visitation_consumer"
        cluster = Cluster([HOST_CASSANDRA])
        session = cluster.connect()
        session.execute("use visitation_consumer")

        # Getting Adgroup id from Cassandra
        adgroup = "select * from visit_tracking_store where adgroup =" + adgroup
        rows = session.execute(adgroup)
        print "My row for Adgroup is", rows
        assert rows != [], "Test case failed while getting Adgroup from Cassandra Visit Tracking Store table"

        # Getting Pixel id from Cassandra
        pixel = "select * from pixel_store where pixel_id =" + pixelid
        rows = session.execute(pixel)
        print "My row for PixelId is", rows
        assert rows != [], "Test case failed while getting PixelId from Cassandra Pixel Store table"


    def norows_from_cassandra(self, adgroup, pixelid):

        # Connecting to Cassandra
        HOST_CASSANDRA = g_cassandra_host
        KEYSPACE = "visitation_consumer"
        cluster = Cluster([HOST_CASSANDRA])
        session = cluster.connect()
        session.execute("use visitation_consumer")

        # Getting Adgroup id from Cassandra
        adgroup = "select * from visit_tracking_store where adgroup =" + adgroup
        rows = session.execute(adgroup)
        print "My row for Adgroup is", rows
        assert rows == [], "Test case failed while getting Adgroup from Cassandra Visit Tracking Store table"

        # Getting Pixel id from Cassandra
        pixel = "select * from pixel_store where pixel_id =" + pixelid
        rows = session.execute(pixel)
        print "My row for PixelId is", rows
        assert rows == [], "Test case failed while getting PixelId from Cassandra Pixel Store table"

    def noadgroup_from_cassandra(self, adgroup, pixelid):

        # Connecting to Cassandra
        HOST_CASSANDRA = g_cassandra_host
        KEYSPACE = "visitation_consumer"
        cluster = Cluster([HOST_CASSANDRA])
        session = cluster.connect()
        session.execute("use visitation_consumer")

        # Getting Adgroup id from Cassandra
        adgroup = "select * from visit_tracking_store where adgroup =" + adgroup
        rows = session.execute(adgroup)
        print "My row for Adgroup is", rows
        assert rows == [], "Test case failed while getting Adgroup from Cassandra Visit Tracking Store table"

        # Getting Pixel id from Cassandra
        pixel = "select * from pixel_store where pixel_id =" + pixelid
        rows = session.execute(pixel)
        print "My row for PixelId is", rows
        assert rows != [], "Test case failed while getting PixelId from Cassandra Pixel Store table"

    def delete_from_cassandra(self,pixelid,adgroup,cookieid,deviceid,ip):

        # Connecting to Cassandra
        HOST_CASSANDRA = g_cassandra_host
        KEYSPACE = "visitation_consumer"
        cluster = Cluster([HOST_CASSANDRA])
        session = cluster.connect()
        session.execute("use visitation_consumer")

        # Delete PixelId,Adgroup,CookieId,DeviceId,IP from Cassandra tables

        if pixelid != '':
            rows = "delete from pixel_store where pixel_id =" + pixelid
            rows = session.execute(rows)

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

        print "Successfully Deleted the Adgroup,Pixelid,Cookie Id,Device Id,IP from tables"