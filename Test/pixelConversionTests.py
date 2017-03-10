from Visitation_Consumer.Util.visitTrackingStoreUtil import *
from Visitation_Consumer.Util.adExposureEventsUtil import *
from Visitation_Consumer.Util.pixelConversionUtil import *


class pixelConversionTests(unittest.TestCase):

  g_adExposureEventsUtil= adExposureEventsUtil()
  g_pixelConversionUtil= pixelConversionUtil()

######################TEST CASES FOR PIXEL CONVERSION EVENTS################################

  def test_positive(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdPositive.tsv")
      self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/Positive.tsv")
      self.g_pixelConversionUtil.validate_from_cassandra("1124126","133325")
      self.g_pixelConversionUtil.delete_from_cassandra("133325","1124126","61aeb33b-d1b8-4626-9852-PCPositive","C8595F9C-958C-440F-AE88-PC","126.78.0.1")

  def test_pctimeless(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdPCtimeLess.tsv")
      self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCTimeLess.tsv")
      self.g_pixelConversionUtil.noadgroup_from_cassandra("1124126", "133325")
      self.g_pixelConversionUtil.delete_from_cassandra("133325", "", "61aeb33b-d1b8-4626-9852-PCTimeless","C8595F9C-958C-440F-AE88-PC", "126.78.0.1")

  def test_within_same_day(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdWithinSameday.tsv")
      self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/WithinSameday.tsv")
      self.g_pixelConversionUtil.validate_from_cassandra("1124126","133325")
      self.g_pixelConversionUtil.delete_from_cassandra("133325","1124126","61aeb33b-d1b8-4626-9852-WithinSameDay","C8595F9C-958C-440F-AE88-PC","126.78.0.1")


  def test_pid(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdPID.tsv")
      self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PID.tsv")
      self.g_pixelConversionUtil.noadgroup_from_cassandra("1128085", "133325")
      self.g_pixelConversionUtil.delete_from_cassandra("133325","", "61aeb33b-d1b8-4626-9852-PID","C8595F9C-958C-440F-AE88-PC", "126.78.0.1")


  def test_pid_cid(self):
       self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdPidCid.tsv")
       self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PidCid.tsv")
       self.g_pixelConversionUtil.validate_from_cassandra("1130350", "133440")
       self.g_pixelConversionUtil.delete_from_cassandra("133440", "1130350", "61aeb33b-d1b8-4626-9852-PidCid","C8595F9C-958C-440F-AE88-PC", "126.78.0.1")

  def test_cid_ip(self):
       self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdCidIp.tsv")
       self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/CidIp.tsv")
       self.g_pixelConversionUtil.noadgroup_from_cassandra("1133570", "133325")
       self.g_pixelConversionUtil.delete_from_cassandra("133325","", "61aeb33b-d1b8-4626-9852-CidIp","C8595F9C-958C-440F-AE88-PC", "126.78.0.8")

  def test_cid(self):
       self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PSAdCid.tsv")
       self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/Cid.tsv")
       self.g_pixelConversionUtil.noadgroup_from_cassandra("1137080", "133325")
       self.g_pixelConversionUtil.delete_from_cassandra("133325","","61aeb33b-d1b8-4626-9852-Cid","C8595F9C-958C-440F-AE88-PC", "126.78.0.8")

  def test_ip(self):
       self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdIp.tsv")
       self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/Ip.tsv")
       self.g_pixelConversionUtil.noadgroup_from_cassandra("1137835", "133325")
       self.g_pixelConversionUtil.delete_from_cassandra("133325","","61aeb33b-d1b8-4626-9852-Ip","C8595F9C-958C-440F-AE88-PC", "126.78.0.9")

  def test_pid_ip(self):
       self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdPidIp.tsv")
       self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PidIp.tsv")
       self.g_pixelConversionUtil.validate_from_cassandra("1137905", "133545")
       self.g_pixelConversionUtil.delete_from_cassandra("133545","1137905","61aeb33b-d1b8-4626-9852-PidIp","C8595F9C-958C-440F-AE88-PC", "126.78.0.10")

  def test_nomatch(self):
       self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdNomatch.tsv")
       self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/Nomatch.tsv")
       self.g_pixelConversionUtil.noadgroup_from_cassandra("1138015", "133325")
       self.g_pixelConversionUtil.delete_from_cassandra("133325","","61aeb33b-d1b8-4626-9852-NoMatch","C8595F9C-958C-440F-AE88-PC", "126.78.0.1")

  def test_nopixelid(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCNoPixel.tsv")
      self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/Nopixel.tsv")
      self.g_pixelConversionUtil.norows_from_cassandra("1124126", "133325")
      self.g_pixelConversionUtil.delete_from_cassandra("", "", "61aeb33b-d1b8-4626-9852-NoPixel","C8595F9C-958C-440F-AE88-PC", "126.78.0.1")

  def test_nocookieid(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdNoCookieId.tsv")
      self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/NoCookieId.tsv")
      self.g_pixelConversionUtil.validate_from_cassandra("1124126", "133325")
      self.g_pixelConversionUtil.delete_from_cassandra("133325", "1124126", "61aeb33b-d1b8-4626-9852-NoCookieId","C8595F9C-958C-440F-AE88-PC", "126.78.0.1")

  def test_noip(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdNoIp.tsv")
      self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/NoIP.tsv")
      self.g_pixelConversionUtil.validate_from_cassandra("1124126", "133325")
      self.g_pixelConversionUtil.delete_from_cassandra("133325", "1124126", "61aeb33b-d1b8-4626-9852-NoIp", "C8595F9C-958C-440F-AE88-PC","126.78.0.1")

  def test_nocookieid_adexp(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdNoCookieIdAdExp.tsv")
      self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/NoCookieIdAdExp.tsv")
      self.g_pixelConversionUtil.validate_from_cassandra("1124126", "133325")
      self.g_pixelConversionUtil.delete_from_cassandra("133325", "1124126", "","C8595F9C-958C-440F-AE88-PC", "126.78.0.1")

  def test_noip_adexp(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdNoIpforAdExp.tsv")
      self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/NoIPforAdExp.tsv")
      self.g_pixelConversionUtil.validate_from_cassandra("1124126", "133325")
      self.g_pixelConversionUtil.delete_from_cassandra("133325", "1124126", "61aeb33b-d1b8-4626-9852-NoIpAdExp", "C8595F9C-958C-440F-AE88-PC","")

  def test_within1sec(self):
      self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdWithin1sec.tsv")
      self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/Within1sec.tsv")
      self.g_pixelConversionUtil.validate_from_cassandra("1124126", "133325")
      self.g_pixelConversionUtil.delete_from_cassandra("133325", "1124126", "61aeb33b-d1b8-4626-9852-Within1sec", "C8595F9C-958C-440F-AE88-PC","126.78.0.1")


  def test_future_time(self):
       self.g_adExposureEventsUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/PCAdFutureTime.tsv")
       self.g_pixelConversionUtil.trigger_events("/media/ephemeral0/xad/VisitationConsumer/PixelConvEvents/FutureTime.tsv")
       self.g_pixelConversionUtil.norows_from_cassandra("1124126", "133325")
       self.g_pixelConversionUtil.delete_from_cassandra("","","61aeb33b-d1b8-4626-9852-NoMatch","C8595F9C-958C-440F-AE88-PC", "126.78.0.1")


if __name__ == '__main__':
    unittest.main()