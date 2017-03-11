from Util.visitTrackingStoreUtil import *
from Util.adExposureEventsUtil import *


class visitTrackingStoreTests(unittest.TestCase):

  g_visitTrackingStoreUtil= visitTrackingStoreUtil()
  g_adExposureEventsUtil= adExposureEventsUtil()


######################TEST CASES FOR VISIT TRACKING EVENTS################################


  def test_brandid_mismatch(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/VSAdBrandIdMismatch.tsv")
      self.g_visitTrackingStoreUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/BrandIdMismatch.tsv")
      self.g_visitTrackingStoreUtil.norows_from_cassandra("823502")
      self.g_visitTrackingStoreUtil.delete_from_cassandra("823502","61aeb33b-d1b8-4626-9852-VS","C8595F9C-958C-440F-BrandIdMismatch","192.48.237.13")

  # def test_future_timestamp(self):
  #     self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/VSAdFutureTimestamp.tsv")
  #     self.g_visitTrackingStoreUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/FutureTimestamp.tsv")
  #     self.g_visitTrackingStoreUtil.norows_from_cassandra("789402")
  #     self.g_visitTrackingStoreUtil.delete_from_cassandra("789402","61aeb33b-d1b8-4626-9852-VS","C8595F9C-958C-440F-FutureTimestamp","192.48.237.13")



  def test_no_timestamp(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/VSAdNoTimestamp.tsv")
      self.g_visitTrackingStoreUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/NoTimeStamp.tsv")
      self.g_visitTrackingStoreUtil.validate_from_cassandra("789475")
      self.g_visitTrackingStoreUtil.delete_from_cassandra("789475", "61aeb33b-d1b8-4626-9852-VS","C8595F9C-958C-440F-AE88-NoTimeStamp", "192.48.237.13")


  def test_positive(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/VSAdPositive.tsv")
      self.g_visitTrackingStoreUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/Positive.tsv")
      self.g_visitTrackingStoreUtil.validate_from_cassandra("787532")
      self.g_visitTrackingStoreUtil.delete_from_cassandra("787532", "61aeb33b-d1b8-4626-9852-VS","C8595F9C-958C-440F-AE88-VisitStorePositive", "192.48.237.13")


  def test_same_timestamp(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/VSAdSameTimeStamp.tsv")
      self.g_visitTrackingStoreUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/SameTimestamp.tsv")
      self.g_visitTrackingStoreUtil.norows_from_cassandra("787534")
      self.g_visitTrackingStoreUtil.delete_from_cassandra("787534", "61aeb33b-d1b8-4626-9852-VS","C8595F9C-958C-440F-AE88-SameTimestamp", "192.48.237.13")

  def test_uid_doesntmatch(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/VSAdUidDoesntMatch.tsv")
      self.g_visitTrackingStoreUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/UidDoesntMatch.tsv")
      self.g_visitTrackingStoreUtil.norows_3para_from_cassandra("790135","1488873600000","AUPT-test-UidDoesntMatch")
      self.g_visitTrackingStoreUtil.delete_from_cassandra("790135", "61aeb33b-d1b8-4626-9852-VS","C8595F9C-958C-440F-AE88-VSAd90days", "192.48.237.13")

  def test_within1day(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/VSAdWithin1day.tsv")
      self.g_visitTrackingStoreUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/VisitStoreEvents/Within1day.tsv")
      self.g_visitTrackingStoreUtil.norows_from_cassandra("789400")
      self.g_visitTrackingStoreUtil.delete_from_cassandra("789400", "61aeb33b-d1b8-4626-9852-VS","C8595F9C-958C-440F-Within1day", "192.48.237.13")


if __name__ == '__main__':
    unittest.main()
