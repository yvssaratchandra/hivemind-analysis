import unittest
from run_compute_recovery import clock_check, replacement_seeds


class RecoveryTests(unittest.TestCase):
    def test_suspension_and_wall_clock_jump_are_rejected(self):
        before = dict(monotonic=10, boot=20, wall=100)
        self.assertTrue(clock_check(before, dict(monotonic=310, boot=320, wall=400))['valid'])
        self.assertFalse(clock_check(before, dict(monotonic=310, boot=440, wall=520))['valid'])
        self.assertFalse(clock_check(before, dict(monotonic=310, boot=320, wall=390))['valid'])

    def test_whole_paired_seeds_selected_independently_of_outcomes(self):
        bad = ['exclusion_s302', 'absent_s302', 'exclusion_s303']
        rows = [dict(case=f'{arm}_s{seed}', timing_eligible=f'{arm}_s{seed}' not in bad)
                for seed in range(302, 307) for arm in ('baseline', 'exclusion', 'absent')]
        self.assertEqual(replacement_seeds(dict(cases=rows, invalidated_cases=bad)), [302, 303])
        with self.assertRaises(ValueError):
            replacement_seeds(dict(cases=rows[:-1], invalidated_cases=bad))
        with self.assertRaises(ValueError):
            replacement_seeds(dict(cases=rows, invalidated_cases=bad[:-1]))


if __name__ == '__main__':
    unittest.main()
