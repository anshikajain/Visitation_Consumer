import unittest
import os.path
import inspect
import sys
from Test.adExposureTests import *
from Test.pixelConversionTests import *
from Test.visitTrackingStoreTests import *

##############################RUNNING ALL TEST SUITES################################################################
class executeTestSuites():

    if __name__ == '__main__':
        test_classes_to_run = [adExposureTests,pixelConversionTests,visitTrackingStoreTests]

        loader = unittest.TestLoader()

        suites_list = []
        for test_class in test_classes_to_run:
            suite = loader.loadTestsFromTestCase(test_class)
            suites_list.append(suite)

        big_suite = unittest.TestSuite(suites_list)

        runner = unittest.TextTestRunner()
        results = runner.run(big_suite)
