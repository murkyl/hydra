# -*- coding: utf8 -*-
import inspect
import random
import os
import sys
import math
import logging
import unittest
import timeit
try:
  import pandas as pd
  ENABLE_PANDAS = True
except:
  ENABLE_PANDAS = False

if __package__ is None:
    current_file = inspect.getfile(inspect.currentframe())
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(current_file)))
    sys.path.insert(0, base_path)
from HistogramStat import HistogramStat
from HistogramStat import HistogramStatCountAndValue
from HistogramStat import HistogramStat2D
from HistogramStat import HashBinCountAndValue

SEED = 42
HIST1 = [5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95]
HIST2 = [ # File size histogram table to see how many files fall within each size range
  0,                    # 0 byte file
  1024,                 # <= 1 kiB
  4096,                 # <= 4 kiB
  8192,                 # <= 8 kiB
  16384,                # <= 16 kiB
  32768,                # <= 32 kiB
  65536,                # <= 64 kiB
  131072,               # <= 128 kiB
  1048576,              # <= 1 MiB
  10485760,             # <= 10 MiB
  104857600,            # <= 100 MiB
  1073741824,           # <= 1 GiB
  10737418240,          # <= 10 GiB
  107374182400,         # <= 100 GiB
  1099511627776,        # <= 1 TiB
  #'other',             # > 1 TiB
]
EXTENSIONS = [None, 'jpg', 'docx', 'pdf', 'tiff', 'gz', 'tar', 'tgz', 'zip', '7z', 'txt', 'xlsx', 'pptx', 'dat']

TEST_BASIC_VALUES = [10, 8, 14, 13, 5, 13, 16, 6, 7, 12, 8, 7, 7, 10, 9, 10, 12, 11, 12, 10]
TEST_HIST_COUNT_AND_VALUE =  [[10, 28], [8, 66], [14, 178], [13, 246], [5, 117], [13, 362], [16, 533], [6, 229], [7, 304], [12, 571], [8, 426], [7, 407], [7, 447], [10, 686], [9, 656], [10, 779], [12, 986], [11, 966], [12, 1116], [10, 973]]
TEST_HIST_COUNT_AND_VALUE_NORM = [[2, 0], [1, 24576], [5, 122880], [7, 172032], [17, 835584], [35, 3047424], [58, 8994816], [75, 20004864], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]
TEST_HIST_2D = [[11, 653312, [[0, 0], [0, 0], [1, 3072], [0, 0], [3, 34816], [0, 0], [0, 0], [7, 615424], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [18, 1016832, [[0, 0], [0, 0], [1, 4096], [0, 0], [2, 24576], [3, 74752], [2, 94208], [10, 819200], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [9, 441344, [[0, 0], [0, 0], [0, 0], [2, 13312], [1, 11264], [1, 27648], [1, 49152], [4, 339968], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [9, 435200, [[0, 0], [0, 0], [0, 0], [0, 0], [1, 16384], [3, 83968], [2, 93184], [3, 241664], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [5, 220160, [[0, 0], [0, 0], [0, 0], [0, 0], [1, 12288], [1, 19456], [2, 118784], [1, 69632], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [15, 807936, [[0, 0], [0, 0], [1, 4096], [1, 7168], [1, 12288], [2, 34816], [3, 169984], [7, 579584], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [17, 840704, [[1, 0], [0, 0], [0, 0], [1, 8192], [1, 9216], [5, 132096], [2, 111616], [7, 579584], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [8, 349184, [[0, 0], [0, 0], [0, 0], [1, 7168], [1, 10240], [2, 58368], [2, 108544], [2, 164864], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [7, 281600, [[0, 0], [0, 0], [0, 0], [0, 0], [3, 36864], [0, 0], [3, 144384], [1, 100352], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [9, 365568, [[1, 0], [0, 0], [0, 0], [0, 0], [1, 12288], [1, 20480], [4, 155648], [2, 177152], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [11, 711680, [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [2, 52224], [3, 146432], [6, 513024], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [12, 704512, [[0, 0], [0, 0], [0, 0], [0, 0], [1, 15360], [1, 17408], [3, 139264], [7, 532480], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [8, 480256, [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [2, 58368], [4, 243712], [2, 178176], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [9, 369664, [[0, 0], [0, 0], [0, 0], [0, 0], [2, 27648], [3, 71680], [2, 74752], [2, 195584], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [7, 355328, [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [3, 78848], [2, 116736], [2, 159744], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [6, 154624, [[0, 0], [0, 0], [1, 3072], [1, 5120], [1, 10240], [0, 0], [3, 136192], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [8, 452608, [[0, 0], [0, 0], [0, 0], [0, 0], [1, 14336], [1, 21504], [3, 168960], [3, 247808], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [9, 564224, [[0, 0], [0, 0], [0, 0], [1, 8192], [0, 0], [1, 25600], [1, 55296], [6, 475136], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [13, 750592, [[0, 0], [0, 0], [0, 0], [0, 0], [2, 27648], [0, 0], [5, 241664], [6, 481280], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]], [9, 487424, [[0, 0], [0, 0], [0, 0], [1, 7168], [0, 0], [2, 51200], [2, 78848], [4, 350208], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]]]
BLOCK_SIZE = 8192
BENCHMARK_DATA = None

def norm(x):
  # Super simple normalization function that takes any file size < 128K
  # and takes the nearest whole number of 8K blocks and multiples it by 3
  blocks = math.ceil(x/BLOCK_SIZE)
  if blocks <= 16:
    return(blocks*3*BLOCK_SIZE)
  else:
    return(BLOCKS*8192)

class TestHistogramStat(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    #print("setUpClass")
    pass

  @classmethod
  def tearDownClass(cls):
    #print("tearDownClass called")
    pass

  #@unittest.skip("")
  def test_001_histogram_stat(self):
    random.seed(SEED)
    dut = HistogramStat(HIST1)
    for i in range(200):
      dut.insert_data(random.randint(0,100))
    hist = dut.get_histogram_array()
    self.assertEqual(hist, TEST_BASIC_VALUES)
      
  #@unittest.skip("")
  def test_002_histogram_stat_count_and_value(self):
    random.seed(SEED)
    dut = HistogramStatCountAndValue(HIST1)
    for i in range(200):
      dut.insert_data(random.randint(0,100))
    hist = dut.get_histogram_array()
    self.assertEqual(hist, TEST_HIST_COUNT_AND_VALUE)
      
  #@unittest.skip("")
  def test_003_histogram_stat_count_and_value_normalized(self):
    random.seed(SEED)
    dut = HistogramStatCountAndValue(HIST2, norm=norm)
    for i in range(200):
      dut.insert_data(random.randint(0,100)*1024)
    hist = dut.get_histogram_array()
    self.assertEqual(hist, TEST_HIST_COUNT_AND_VALUE_NORM)
      
  #@unittest.skip("")
  def test_004_histogram_2D(self):
    random.seed(SEED)
    dut = HistogramStat2D(HIST1, HIST2, norm=norm)
    dataset = []
    for i in range(200):
      data = [random.randint(0,100), random.randint(0,100)*1024]
      dataset.append(data)
      dut.insert_data(data[0], data[1])
    dataset.sort(key=lambda x:x[0])
    #print(dataset)
    #print("Output")
    #hist = dut.get_histogram()
    #for key in list(hist.keys()):
    #  if hist[key][0] != 0:
    #    print("Bin: %s, Count: %s, Total: %s, Hist: %s"%(key, hist[key][0], hist[key][1], hist[key][2].get_histogram_array()))
    hist = dut.get_histogram_array()
    self.assertEqual(hist, TEST_HIST_2D)
    
  #@unittest.skip("")
  def test_006_hash_bin(self):
    random.seed(SEED)
    dut = HashBinCountAndValue()
    for i in range(200):
      dut.insert_data(EXTENSIONS[random.randrange(0, len(EXTENSIONS))], random.randint(0,100)*1024)
    hist = dut.get_bins()
    print(hist)
    #self.assertEqual(hist, TEST_BASIC_VALUES)
      
  @unittest.skip("")
  def test_102_benchmark_histogram_stat_count_and_value(self):
    INNER_LOOP_COUNT = 1000000
    OUTER_LOOP_COUNT = 1
    total_time = 0
    random.seed(SEED)
    dut = HistogramStatCountAndValue(HIST1, cache=10240)
    #dut = HistogramStat(HIST1, cache=10240)
    BENCHMARK_DATA = [0]*INNER_LOOP_COUNT
    for j in range(OUTER_LOOP_COUNT):
      for i in range(INNER_LOOP_COUNT):
        BENCHMARK_DATA[i] = random.randint(0,100)
      if ENABLE_PANDAS:
        df_nums = {}
      else:
        df_nums = {}
      def closure():
        for x in BENCHMARK_DATA:
          dut.insert_data(x)
      if ENABLE_PANDAS:
        def closure2():
          df_nums['data'] = pd.cut(x=BENCHMARK_DATA, bins=[0] + HIST1 + [999999999])
      bench_result = timeit.timeit(stmt=closure, number=1)
      if ENABLE_PANDAS:
        bench_result2 = timeit.timeit(stmt=closure2, number=1)
      total_time += bench_result
      print("Bench result for iteration %d: %s"%(j, bench_result))
      if ENABLE_PANDAS:
        print("Bench2 result for iteration %d: %s"%(j, bench_result2))
    avg_time = total_time/(OUTER_LOOP_COUNT * 1.0)
    print("Average time for %d insertions: %f"%(INNER_LOOP_COUNT, avg_time))
    hist = dut.get_histogram_array()
    print(hist)
    if ENABLE_PANDAS:
      print(df_nums['data'].value_counts())
    
    self.assertTrue(avg_time < 1.0)
    
      
if __name__ == '__main__':
  root = logging.getLogger()
  root.setLevel(logging.WARN)
  #root.setLevel(logging.DEBUG)
  #root.setLevel(5)
  #root.setLevel(9)

  ch = logging.StreamHandler(sys.stdout)
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s')
  ch.setFormatter(formatter)
  root.addHandler(ch)

  suite1 = unittest.TestLoader().loadTestsFromTestCase(TestHistogramStat)
  all_tests = unittest.TestSuite([suite1])
  unittest.TextTestRunner(verbosity=2).run(all_tests)
  
  