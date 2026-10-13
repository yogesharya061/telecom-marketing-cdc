import unittest
from app.policy import current_reason, validate

class PolicyTest(unittest.TestCase):
    def setUp(self):
        self.row = dict(billing_cycle='2026-10',marketing_consent=True,account_status='ACTIVE',used_mb=800,quota_mb=1000)
    def test_threshold_boundaries(self):
        for used,expected in [(799,'BELOW_THRESHOLD'),(800,'ELIGIBLE'),(1200,'ELIGIBLE')]:
            self.row['used_mb']=used
            self.assertEqual(current_reason(self.row,'2026-10'),expected)
    def test_consent_wins(self):
        self.row['marketing_consent']=False
        self.assertEqual(current_reason(self.row,'2026-10'),'NO_CONSENT')
    def test_deleted(self):
        self.assertEqual(current_reason(None,'2026-10'),'DELETED')
    def test_cycle(self):
        self.assertEqual(current_reason(self.row,'2026-09'),'STALE_CYCLE')
    def test_suspended(self):
        self.row['account_status']='SUSPENDED'
        self.assertEqual(current_reason(self.row,'2026-10'),'INACTIVE')
    def test_contract(self):
        event=dict(event_id='x',customer_id='c',billing_cycle='2026-10',campaign_id='DATA_TOPUP_V1',reason='ELIGIBLE',revision=1)
        self.assertEqual(validate(event),event)
        for patch in [dict(revision=True),dict(reason='INVALID'),dict(customer_id=None),dict(campaign_id='OTHER')]:
            with self.assertRaises(ValueError):
                validate(event|patch)
    def test_nonobject(self):
        for value in [None,[],42,'hello']:
            with self.assertRaises(ValueError):
                validate(value)

if __name__ == '__main__':
    unittest.main()
