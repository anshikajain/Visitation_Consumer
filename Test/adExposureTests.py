import unittest
import inspect
import sys
import os
from Util.adExposureEventsUtil import *
import csv


class adExposureTests(unittest.TestCase):

  g_adExposureEventsUtil= adExposureEventsUtil()


######################TEST CASES FOR AD EXPOSURE EVENTS################################

  def test_action_type_imp(self):
        self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/AdExposureEvents/ActionTypeImp.tsv")
        self.g_adExposureEventsUtil.validate_from_cassandra("61aeb33b-d1b8-4626-9852-actiontypeimp","C8595F9C-958C-440F-AE88-actiontypeimp","66.25.28.237")


  def test_future_timestamp(self):
        self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/AdExposureEvents/FutureTimestamp.tsv")
        self.g_adExposureEventsUtil.norows_from_cassandra("61aeb33b-d1b8-4626-9852-FutureTimestamp",
                                                            "C8595F9C-958C-440F-AE88-FutureTimestamp", "12.23.56.78")

  def test_invalid_adgroup(self):
        self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/AdExposureEvents/InvalidAdGroup.tsv")
        self.g_adExposureEventsUtil.norows_from_cassandra("61aeb33b-d1b8-4626-9852-InvalidAdgroup",
                                                            "C8595F9C-958C-440F-AE88-InvalidAdgroup", "134.76.45.12")

  def test_no_adgroup(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/AdExposureEvents/NoAdGroup.tsv")
      self.g_adExposureEventsUtil.norows_from_cassandra("61aeb33b-d1b8-4626-9852-NoAdGroup",
                                                            "C8595F9C-958C-440F-AE88-NoAdGroup", "234.70.1.0")

  def test_no_cookieid(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/AdExposureEvents/NoCookieId.tsv")
      self.g_adExposureEventsUtil.validate_from_cassandra("","C8595F9C-958C-440F-AE88-NoCookieId", "67.187.94.98")

  def test_no_ip(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/AdExposureEvents/NoIP.tsv")
      self.g_adExposureEventsUtil.validate_from_cassandra("61aeb33b-d1b8-4626-9852-NoIP","C8595F9C-958C-440F-AE88-NoIP", "")

  def test_no_uid(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/AdExposureEvents/Nouid.tsv")
      self.g_adExposureEventsUtil.validate_from_cassandra("61aeb33b-d1b8-4626-9852-NoUid","", "72.129.131.248")

  def test_positive(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/AdExposureEvents/Positive.tsv")
      self.g_adExposureEventsUtil.validate_from_cassandra("61aeb33b-d1b8-4626-9852-positive","C8595F9C-958C-440F-AE88-positive", "192.48.236.70")

  def test_timestamp(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/AdExposureEvents/TimeStamp.tsv")
      self.g_adExposureEventsUtil.validate_from_cassandra("61aeb33b-d1b8-4626-9852-Timestamp","C8595F9C-958C-440F-AE88-Timestamp", "70.192.67.0")

  def test_uidtype_gidfa(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/AdExposureEvents/uidTypeGIDFA.tsv")
      self.g_adExposureEventsUtil.validate_from_cassandra("61aeb33b-d1b8-4626-9852-gidfa","C8595F9C-958C-440F-AE88-gidfa", "216.147.153.0")

  def test_morethan30days(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/VSAd30days.tsv")
      self.g_adExposureEventsUtil.norows_from_cassandra("61aeb33b-d1b8-4626-9852-VS","C8595F9C-958C-440F-AE88-MoreThan30days","192.48.237.13")

if __name__ == '__main__':
    unittest.main()
