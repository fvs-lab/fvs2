import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("ci", Path(__file__).with_name("ci.py"))
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


class ComplianceGateTests(unittest.TestCase):
    log = "CVE findings: 0\nNative SAST completed: found 0 issues\n"
    empty = {"bomFormat": "CycloneDX", "vulnerabilities": [], "components": [{"name": "dependency"}]}

    def test_complete_clean_scan_passes(self):
        self.assertTrue(ci.compliance_result(self.empty, self.log)["ok"])

    def test_each_source_of_findings_fails(self):
        sbom = {"bomFormat": "CycloneDX", "vulnerabilities": [{"id": "SAST-test"}], "components": [{"name": "dependency"}]}
        for report, log in ((sbom, self.log), (self.empty, self.log.replace("findings: 0", "findings: 1")),
                            (self.empty, self.log.replace("found 0", "found 1"))):
            with self.subTest(report=report, log=log):
                self.assertFalse(ci.compliance_result(report, log)["ok"])

    def test_incomplete_or_offline_scan_fails(self):
        for log in ("", "CVE findings: 0", self.log + "OSV/EUVD query failed", self.log + "[ERROR] catalog failed"):
            self.assertFalse(ci.compliance_result(self.empty, log)["ok"])
        self.assertFalse(ci.compliance_result(self.empty, self.log, 1)["ok"])
        self.assertFalse(ci.compliance_result({"bomFormat": "CycloneDX", "components": []}, self.log)["ok"])

    def test_invalid_sbom_is_rejected(self):
        for report in ({}, {"bomFormat": "CycloneDX", "vulnerabilities": "invalid"}):
            with self.assertRaises(ValueError):
                ci.compliance_result(report, self.log)


if __name__ == "__main__":
    unittest.main()
