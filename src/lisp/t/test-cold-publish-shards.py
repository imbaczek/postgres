import itertools
from pathlib import Path
import runpy
import unittest

api = runpy.run_path(str(Path(__file__).with_name('logical-slot-copy-cold-publish.py')))
cut_cases = api['cut_cases']


class CutShards(unittest.TestCase):
    def test_shards_partition_the_complete_fault_inventory(self):
        for flags in (['--cuts'], ['--crashes'], ['--cuts', '--crashes']):
            full = cut_cases(flags)
            for count in range(1, len(full)+1):
                shards = [cut_cases(['--smoke', *flags], f'{index}/{count}')
                          for index in range(count)]
                self.assertTrue(all(shards))
                flattened = list(itertools.chain.from_iterable(shards))
                self.assertEqual(len(flattened), len(set(flattened)))
                self.assertCountEqual(flattened, full)

    def test_bad_shards_cannot_silently_select_no_tests(self):
        for spec in ('', '0', '0/0', '-1/3', '3/3', '0/21', 'x/3', '0/2/3'):
            with self.subTest(spec=spec), self.assertRaises(ValueError):
                cut_cases(['--smoke', '--cuts'], spec)
        for flags in ([], ['--smoke'], ['--cuts'], ['--crashes']):
            with self.subTest(flags=flags), self.assertRaises(ValueError):
                cut_cases(flags, '0/2')


if __name__ == '__main__':
    unittest.main()
