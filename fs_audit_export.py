# -*- coding: utf8 -*-
# -*- coding: utf8 -*-
__title__       = "fs_audit_export"
__date__        = "06 March 2020"
__version__     = "1.0.0"
__status__      = "Beta"
__author__      = "Andrew Chung <acchung@gmail.com>"
__maintainer__  = "Andrew Chung <acchung@gmail.com>"
__email__       = "acchung@gmail.com"
__credits__     = []
__all__         = [
  'export_xlsx',
]
__license__ = "MIT"
__copyright__ = """Copyright 2020 Andrew Chung
Permission is hereby granted, free of charge, to any person obtaining a copy of 
this software and associated documentation files (the "Software"), to deal in 
the Software without restriction, including without limitation the rights to 
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies 
of the Software, and to permit persons to whom the Software is furnished to do 
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all 
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
SOFTWARE."""
__description__="""====================
Requirements:
  python 2.7+

Used by the fs_audit script to export the audit data to an Excel workbook.

====================
"""

import os
import sys
import math
import logging
import traceback
import HistogramStat
import lib.xlsxwriter as xlsxwriter

DEFAULT_CFG = {
  'top_n': 100,
}
CELL_FORMAT = {}
CELL_FORMAT_STRINGS = {
  None: {},
  'header1': {'align': 'center', 'text_wrap': True, 'bold': True},
  'align_right': {'align': 'right'},
  'group_num1': {'align': 'right', 'num_format': '#,##0'},
  '0_decimal_pct': {'align': 'right', 'num_format': '0%'},
  '2_decimal': {'align': 'right', 'num_format': '#0.00'},
  '2_decimal_pct': {'align': 'right', 'num_format': '0.00%'},
  '4_decimal': {'align': 'right', 'num_format': '0.0000'},
  'base10_gb': {'align': 'right', 'num_format': '[<1000000]0.00," KB";[<1000000000]0.00,," MB";0.00,,," GB"'},
  'base2_gb': {'align': 'right', 'num_format': '[<1048576]0.00," KiB";[<1073741824]0.00,," MiB";0.00,,," GiB"'},
  'gt_base10_gb': {'align': 'right', 'num_format': '[<1000000]"> "#0.00," KB";[<1000000000]"> "#0.00,," MB";"> "#0.00,,," GB"'},
  'gt_base2_gb': {'align': 'right', 'num_format': '[<1048576]"> "#0.00," KiB";[<1073741824]"> "#0.00,," MiB";"> "#0.00,,," GiB"'},
  'lt_base10_gb': {'align': 'right', 'num_format': '[<1000000]"<= "#0.00," KB";[<1000000000]"<= "#0.00,," MB";"<= "#0.00,,," GB"'},
  'lt_base2_gb': {'align': 'right', 'num_format': '[<1048576]"<= "#0.00," KiB";[<1073741824]"<= "#0.00,," MiB";"<= "#0.00,,," GiB"'},
}

def get_cell_format(workbook, format_name, format_dict=None):
  format = CELL_FORMAT.get(format_name)
  if format:
    return format
  format = CELL_FORMAT_STRINGS.get(format_name)
  if not format:
    if not format_dict:
      return None
    CELL_FORMAT_STRINGS[format_name] = format_dict
  CELL_FORMAT[format_name] = workbook.add_format(format)
  return CELL_FORMAT[format_name]

def get_y_axis_max(type, value):
  max = 1
  if type == 'file_size':
    # We assume the values are in GB. We want some reasonable numbers for the max
    for i in [10000, 1000, 100, 10, 1]:
      range = value//i
      if range > 0:
        max = math.ceil(value/i)*i
        break
    if i == 1:
      if value < 1:
        max = round(1.2*value, 4)
  elif type == 'file_count':
    for i in [100000, 10000, 1000, 100, 10, 1]:
      range = value//i
      if range > 0:
        max = math.ceil(value/i)*i
        break
    if i == 1:
      max = 1
  return max

"""humanize takes a string and does a simple text translation to present a user
with a more normal representation of the string.
For example:
lacp -> LACP
round_robin -> Round Robin

It also translates a bollean value into a string as well as replacing any '_'
characters with a space.
"""
def humanize(s):
  str_map = {
    'source_code': 'Source Code',
  }
  if isinstance(s, bool):
    s = str(s)
  if s is None:
    s = ''
  try:
    s = s.replace('_', ' ')
  except:
    return ''
  ret_val = str_map.get(s, None)
  if ret_val:
    return ret_val
  return s.capitalize()

def humanize_number(num, suffix='B', base=10, truncate=True):
  num = num if num else 0
  factor = 1024.0 if base == 2 else 1000.0
  bin_mark = ''
  if num == 0:
    return "0 %s"%(suffix)
  for unit in ['', 'K', 'M', 'G', 'T', 'P' ,'E' ,'Z' , 'Y']:
    if abs(num) < factor:
      break
    num /= factor
  if unit != '' and base == 2:
    bin_mark = 'i'
  return "%.1f %s%s%s"%(num, unit, bin_mark, suffix)

def humanize_time(num_sec):
  FACTORS = {
    'year': 365*24*3600,
    'week': 7*24*3600,
    'day': 24*3600,
    'hour': 3600,
    'minute': 60,
  }
  time_str = ''
  try:
    num_sec*1
  except:
    return num_sec
  
  for factor in FACTORS.keys():
    if num_sec//FACTORS[factor] > 0:
      whole = num_sec//FACTORS[factor]
      if (factor == 'week') and (whole >= 4):
        factor = 'day'
        whole = num_sec//FACTORS[factor]
      time_str += '%d %s%s '%(whole, factor, 's' if whole > 1 else '')
      num_sec -= whole*FACTORS[factor]
  time_str = time_str.strip()
  if num_sec:
    time_str = time_str + '%s second%s'%(num_sec, 's' if num_sec > 1 else '')
  else:
    if not time_str:
      time_str = '0 seconds'
  return time_str

def write_column_headers(headers, document, worksheet, row=0, col=0, row_height=28, col_width=20):
  worksheet.set_row(row, row_height, get_cell_format(document, 'header1'))
  for i in  range(len(headers)):
    cell_format = get_cell_format(document, headers[i].get('f'))
    worksheet.set_column(col+i, col+i, headers[i].get('c', col_width), cell_format)
  worksheet.write_row(row, col, [x['t'] for x in headers])

def insert_category_def(data, document, worksheet=None, cfg={}, row=0, col=0):
  headers = [{'t': humanize(x)} for x in HistogramStat.CATEGORIES.keys()]

  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  for key in HistogramStat.CATEGORIES.keys():
    worksheet.write_column(row, col, HistogramStat.CATEGORIES[key])
    col += 1

def insert_file_age_distribution(data, document, worksheet=None, cfg={}, row=0, col=0):
  gbasic = lambda x, y='': data.get('basic', {}).get(x, y)
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  headers = [
      {'t': 'File age bins',                        'c': 25},
      {'t': 'Last access time',                     'c': 25,  'f': 'align_right'},
      {'t': 'Last access\n% of total',              'c': 25,  'f': '2_decimal_pct'},
      {'t': 'Create/Last meta change',              'c': 25,  'f': 'align_right'},
      {'t': 'Create/Last meta change\n% of total',  'c': 25,  'f': '2_decimal_pct'},
      {'t': 'Last data modified',                   'c': 25,  'f': 'align_right'},
      {'t': 'Last data modified\n% of total',       'c': 25,  'f': '2_decimal_pct'},
  ]
  total_files = gbasic('processed_files')
  adata = gdetail('hist_file_count_by_atime')
  cdata = gdetail('hist_file_count_by_ctime')
  mdata = gdetail('hist_file_count_by_mtime')
  if not adata or not cdata or not mdata:
    logging.getLogger().error('One of the file age histograms is empty. Both should have data.')
    return
  bins = adata.get_bin_config()
  ahist = adata.get_histogram()
  chist = cdata.get_histogram()
  mhist = mdata.get_histogram()

  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  
  for bin in bins:
    worksheet.write_row(
      row,
      col,
      [
        humanize_time(bin),
        ahist[bin][0], ahist[bin][0]/total_files,
        chist[bin][0], chist[bin][0]/total_files,
        mhist[bin][0], mhist[bin][0]/total_files,
      ]
    )
    row += 1
    
  chartsheet = document.add_chartsheet('%s chart'%worksheet.get_name())
  chart = document.add_chart({'type': 'line'})
  chart.add_series({
      'name': 'Access time',
      'categories': [worksheet.get_name(), 1, col, row, col],
      'values': [worksheet.get_name(), 1, col+2, row, col+2],
      'marker': {'type': 'diamond'},
  })
  chart.add_series({
      'name': 'Create/Metadata change time',
      'categories': [worksheet.get_name(), 1, col, row, col],
      'values': [worksheet.get_name(), 1, col+4, row, col+4],
      'marker': {'type': 'diamond'},
  })
  chart.add_series({
      'name': 'Modified time',
      'categories': [worksheet.get_name(), 1, col, row, col],
      'values': [worksheet.get_name(), 1, col+6, row, col+6],
      'marker': {'type': 'diamond'},
  })
  chart.set_y_axis({
      'name': '% of total files',
      'min': 0,
      'max': 1,
      'num_format': CELL_FORMAT_STRINGS['0_decimal_pct']['num_format'],
  })
  chartsheet.set_chart(chart)

def insert_file_categories_by_size(data, document, worksheet=None, cfg={}, row=0, col=0):
  gbasic = lambda x, y='': data.get('basic', {}).get(x, y)
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  nbase = gconf('number_base', 2)
  size_str = 'base10_gb' if nbase == 10 else 'base2_gb'
  headers = [
      {'t': 'File category',                  'f': 'align_right'},
      {'t': 'File count',                     'f': 'align_right'},
      {'t': 'File size',            'c': 30,  'f': size_str},
      {'t': '% of total capacity',  'c': 30,  'f': '2_decimal_pct'},
  ]
  bins = data['detailed']['category'].get_bins()
  keys = list(bins.keys())
  key_order = sorted(keys, key=lambda x: bins[x]['total'], reverse=True)

  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  for key in key_order:
    worksheet.write_row(
      row,
      col,
      [humanize(key), bins[key]['count'], bins[key]['total'], bins[key]['total']/gbasic('file_size_total')])
    row += 1
    if row > cfg.get('top_n', float('inf')):
      break

def insert_file_extensions_by_size(data, document, worksheet=None, cfg={}, row=0, col=0):
  gbasic = lambda x, y='': data.get('basic', {}).get(x, y)
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  nbase = gconf('number_base', 2)
  size_str = 'base10_gb' if nbase == 10 else 'base2_gb'
  headers = [
      {'t': 'File extension',                 'f': 'align_right'},
      {'t': 'File count',                     'f': 'align_right'},
      {'t': 'File size',            'c': 30,  'f': size_str},
      {'t': '% of total capacity',  'c': 30,  'f': '2_decimal_pct'},
  ]
  bins = data['detailed']['extensions'].get_bins()
  keys = list(bins.keys())
  key_order = sorted(keys, key=lambda x: bins[x]['total'], reverse=True)

  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  for key in key_order:
    worksheet.write_row(
      row,
      col,
      [key or '<No extension>', bins[key]['count'], bins[key]['total'], bins[key]['total']/gbasic('file_size_total')]
    )
    row += 1
    if row > cfg.get('top_n', float('inf')):
      break

def insert_file_sizes_histogram(data, document, worksheet=None, cfg={}, row=0, col=0):
  gbasic = lambda x, y='': data.get('basic', {}).get(x, y)
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  nbase = gconf('number_base', 2)
  gb_size_str = 'GB' if nbase == 10 else 'GiB'
  size_str = 'base10_gb' if nbase == 10 else 'base2_gb'
  headers = [
      {'t': 'File size bins',                                       'f': 'lt_'+size_str},
      {'t': 'File count',                                           'f': 'group_num1'},
      {'t': 'File size (bytes)',                                    'f': size_str},
      {'t': '% of total capacity',                        'c': 30,  'f': '2_decimal_pct'},
      {'t': 'File size (%s)'%gb_size_str,                           'f': '4_decimal'},
      {'t': 'File count\nBlock boundary',                           'f': 'group_num1'},
      {'t': 'File size (bytes)\nBlock boundary',                    'f': size_str},
      {'t': '% of total capacity\nBlock boundary',        'c': 30,  'f': '2_decimal_pct'},
      {'t': 'File size (%s)\nBlock boundary'%gb_size_str,           'f': '4_decimal'},
  ]
  div = 1000**3 if nbase == 10 else 1024**3
  total_file_size = gbasic('file_size_total')
  total_file_size_block = gbasic('file_size_block_total')
  max_size = 0
  max_size_block = 0
  max_file_count = 0
  max_file_count_block = 0
  

  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  file_size_hist = gdetail('hist_file_count_by_size')
  file_block_size_hist = gdetail('hist_file_count_by_block_size')
  if not file_size_hist or not file_block_size_hist:
    logging.getLogger().error('One of the file size histograms is empty. Both should have data.')
    return
  hist1 = file_size_hist.get_histogram()
  hist2 = file_block_size_hist.get_histogram()
  
  last_key = None
  for key in hist1.keys():
    bin = hist1[key]
    bin2 = hist2[key]
    if key != 'other':
      title = key
    else:
      title = last_key
      get_cell_format(document, 'lt_'+size_str)
    file_size = bin[1]/div
    file_size_block = bin2[1]/div
    if file_size > max_size:
      max_size = file_size
    if file_size_block > max_size_block:
      max_size_block = file_size_block
    if bin[0] > max_file_count:
      max_file_count = bin[0]
    if bin2[0] > max_file_count_block:
      max_file_count_block = bin2[0]
    worksheet.write_row(
        row, col,
        [
          title,
          bin[0], bin[1], bin[1]/total_file_size, file_size,
          bin2[0], bin2[1], bin2[1]/total_file_size_block, file_size_block
        ]
    )
    row += 1
    last_key = key
  # Re-write last bin title
  worksheet.write(row-1, col, title, get_cell_format(document, 'gt_'+size_str))
  
  worksheet.write_formula(row+1, col+1, '=sum(B2:B%d)'%row, None, gbasic('total_files'))
  worksheet.write_formula(row+1, col+2, '=sum(C2:C%d)'%row, None, total_file_size)
  worksheet.write_formula(row+1, col+4, '=sum(E2:E%d)'%row, None, total_file_size/div)
  worksheet.write_formula(row+1, col+5, '=sum(F2:F%d)'%row, None, gbasic('total_files'))
  worksheet.write_formula(row+1, col+6, '=sum(G2:G%d)'%row, None, total_file_size_block)
  worksheet.write_formula(row+1, col+8, '=sum(I2:I%d)'%row, None, total_file_size_block/div)
  chartsheet = document.add_chartsheet('%s chart'%worksheet.get_name())
  # TODO: Need to update the y axis max if both normal and block are on the same chart
  size_max = get_y_axis_max('file_size', max_size)
  file_max = get_y_axis_max('file_count', max_file_count)
  if size_max <= 1:
    data_label_format = CELL_FORMAT_STRINGS['4_decimal']['num_format']
  elif size_max < 5:
    data_label_format = CELL_FORMAT_STRINGS['2_decimal']['num_format']
  else:
    data_label_format = CELL_FORMAT_STRINGS['group_num1']['num_format']
  chart = document.add_chart({'type': 'column'})
  chart.set_legend({'position': 'bottom'})
  chart.add_series({
      'name': 'Used capacity (%s)'%gb_size_str,
      'categories': [worksheet.get_name(), 1, col, row, col],
      'values': [worksheet.get_name(), 1, col+4, row, col+4],
      'data_labels': {'value': True, 'num_format': data_label_format},
  })
  # TODO: Uncomment to merge block based counts
  #chart.add_series({
  #    'name': 'Used capacity (%s) (block)'%gb_size_str,
  #    'categories': [worksheet.get_name(), 1, col, row, col],
  #    'values': [worksheet.get_name(), 1, col+8, row, col+8],
  #    'data_labels': {'value': False, 'num_format': data_label_format},
  #})
  chart.set_y_axis({
      'name': 'Capacity (%s)'%gb_size_str,
      'min': 0,
      'max': size_max,
  })
  chart2 = document.add_chart({'type': 'line'})
  chart2.add_series({
      'name': 'File count',
      'categories': [worksheet.get_name(), 1, col, row, col],
      'values': [worksheet.get_name(), 1, col+1, row, col+1],
      'marker': {'type': 'diamond'},
      'y2_axis': True,
  })
  # TODO: Uncomment to merge block based counts
  #chart2.add_series({
  #    'name': 'File count (block)',
  #    'categories': [worksheet.get_name(), 1, col, row, col],
  #    'values': [worksheet.get_name(), 1, col+5, row, col+5],
  #    'marker': {'type': 'square'},
  #    'y2_axis': True,
  #})
  chart2.set_y2_axis({
      'name': 'Number of files',
      'min': 0,
      'max': file_max,
  })
  chart.combine(chart2)
  chartsheet.set_chart(chart)
  
def insert_summary(data, document, worksheet=None, cfg={}, row=0, col=0):
  gbasic = lambda x, y='': data.get('basic', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  nbase = gconf('number_base', 2)
  size_str = 'base10_gb' if nbase == 10 else 'base2_gb'
  dataset = [
    {'t': 'File system summary'},
    {'l': 'Total files scanned',            'd': gbasic('processed_files')},
    {'l': 'Total directories scanned',      'd': gbasic('processed_dirs')},
    {'l': 'Total non leaf directories',     'd': gbasic('parent_dirs_total')},
    {'l': 'Total bytes processed',          'd': gbasic('file_size_total'),         'f': size_str},
    {'l': 'Total bytes processed (block)',  'd': gbasic('file_size_block_total'),   'f': size_str},
    {'l': 'Symlink files',                  'd': gbasic('symlink_files')},
    {'l': 'Skipped files',                  'd': gbasic('skipped_files')},
    {'l': 'Filtered files',                 'd': gbasic('filtered_files')},
    {'l': 'Filtered directories',           'd': gbasic('filtered_dirs')},
    {'l': 'Max files in a directory',       'd': gbasic('max_files_in_dir')},
    {'l': 'Deepest directory level',        'd': gbasic('max_dir_depth')},
    {'l': 'Widest directory level',         'd': gbasic('max_dir_width')},
    {'l': 'Average file size',              'd': gbasic('average_file_size'),       'f': size_str},
    {'l': 'Average file size (block)',      'd': gbasic('average_file_size_block'), 'f': size_str},
    {'l': 'Average directory depth',        'd': gbasic('average_directory_depth'), 'f': '2_decimal'},
    {'l': 'Average directory width',        'd': gbasic('average_directory_width'), 'f': '2_decimal'},
    {'l': 'Block size (bytes)',             'd': gconf('block_size')},
  ]
  
  worksheet.set_column(col, col, 30)
  worksheet.set_column(col+1, col+1, 10, get_cell_format(document, 'align_right'))
  for x in dataset:
    if x.get('t'):
      worksheet.merge_range(row, col, row, col+1, x['t'], get_cell_format(document, 'header1'))
    else:
      data = x.get('d', '')
      worksheet.write(row, col, x.get('l', ''))
      worksheet.write(row, col + 1, data, get_cell_format(document, x.get('f')))
    row += 1

def insert_top_n_files(data, document, worksheet=None, cfg={}, row=0, col=0):
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  nbase = gconf('number_base', 2)
  size_str = 'base10_gb' if nbase == 10 else 'base2_gb'
  headers = [
      {'t': 'File size (bytes)',            'f': size_str},
      {'t': 'File path',          'c': 80,  'f': None},
  ]
  rlist = gdetail('top_n_file_size').get_rank_list()

  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  for i in range(len(rlist) - 1, -1, -1):
    worksheet.write_row(
      row, col,
      [rlist[i][0], rlist[i][1]['filename']]
    )
    row += 1

def insert_top_n_files_per_sid(data, document, worksheet=None, cfg={}, row=0, col=0):
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  nbase = gconf('number_base', 2)
  size_str = 'base10_gb' if nbase == 10 else 'base2_gb'
  headers = [
      {'t': 'File size (bytes)',            'f': size_str},
      {'t': 'File path',          'c': 80,  'f': None},
  ]
  # Get the top N SIDS and then show the top files for that SID
  sid_bins = gdetail('total_by_sid').get_bins()
  sid_keys = list(sid_bins.keys())
  sid_key_order = sorted(sid_keys, key=lambda x: sid_bins[x]['total'], reverse=True)
  bins = gdetail('top_n_file_size_by_sid').get_rank_list()
  
  # Create the special 2 row headers
  start_col = col
  for sid in sid_key_order:
    worksheet.merge_range(row, start_col, row, start_col+1, sid, get_cell_format(document, 'header1'))
    write_column_headers(headers, document, worksheet, row+1, start_col)
    start_col += 3
  row += 2
  
  start_row = row
  for sid_key in sid_key_order:
    user_list = bins[sid_key]
    for i in range(len(user_list) - 1, -1, -1):
      worksheet.write_row(
        row, col,
        [user_list[i][0], user_list[i][1]['filename'], ' '],
      )
      row += 1
    col += 3
    row = start_row

def insert_top_use_by_sid(data, document, worksheet=None, cfg={}, row=0, col=0):
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  nbase = gconf('number_base', 2)
  size_str = 'base10_gb' if nbase == 10 else 'base2_gb'
  headers = [
      {'t': 'User (SID)',         'c': 60,  'f': None},
      {'t': 'File count',                   'f': 'align_right'},
      {'t': 'File size (bytes)',            'f': size_str},
  ]
  bins = gdetail('total_by_sid').get_bins()
  keys = list(bins.keys())
  key_order = sorted(keys, key=lambda x: bins[x]['total'], reverse=True)

  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  for key in key_order:
    worksheet.write_row(
      row, col,
      [key, bins[key]['count'], bins[key]['total']]
    )
    row += 1
    if row > cfg.get('top_n', float('inf')):
      break

def insert_top_use_by_uid(data, document, worksheet=None, cfg={}, row=0, col=0):
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  nbase = gconf('number_base', 2)
  size_str = 'base10_gb' if nbase == 10 else 'base2_gb'
  headers = [
      {'t': 'User (UID)',         'c': 30,  'f': None},
      {'t': 'File count',                   'f': 'align_right'},
      {'t': 'File size (bytes)',            'f': size_str},
  ]
  bins = gdetail('total_by_uid').get_bins()
  keys = list(bins.keys())
  key_order = sorted(keys, key=lambda x: bins[x]['total'], reverse=True)

  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  for key in key_order:
    worksheet.write_row(
      row, col,
      [str(key), bins[key]['count'], bins[key]['total']]
    )
    row += 1
    if row > cfg.get('top_n', float('inf')):
      break

def export_xlsx(data, file):
  modules = [
    [insert_summary, 'Summary'],
    [insert_file_sizes_histogram, 'File sizes'],
    [insert_file_age_distribution, 'File ages'],
    [insert_file_extensions_by_size, 'File extensions'],
    [insert_file_categories_by_size, 'File categories'],
    [insert_top_n_files, 'Top N files'],
    [insert_top_n_files_per_sid, 'Top N files per SID'],
    [insert_top_use_by_sid, 'Use by SID'],
    [insert_top_use_by_uid, 'Use by UID'],
    [insert_category_def, 'Category definition'],
  ]

  workbook = xlsxwriter.Workbook(file)
  cfg = dict(DEFAULT_CFG)
  for mod in modules:
    try:
      worksheet = workbook.add_worksheet(mod[1] if mod[1] else None)
      mod[0](data, workbook, worksheet, cfg)
    except Exception as e:
      logging.getLogger().error('Unable to fully output worksheet: %s\n%s'%(mod[1], traceback.format_exc()))
  try:
    workbook.close()
  except Exception as e:
    logging.getLogger().error('Unable to write XLSX file: %s'%file)
    raise
