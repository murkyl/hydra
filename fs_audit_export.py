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
        max = math.ceil(value/float(i))*i
        break
    if i == 1:
      if value < 1:
        max = round(1.2*value, 4)
  elif type == 'file_count':
    for i in [100000, 10000, 1000, 100, 10, 1]:
      range = value//i
      if range > 0:
        max = math.ceil(value/float(i))*i
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

def helper_insert_top_n_files_per(data, document, bin_type, top_n_type, worksheet=None, cfg={}, row=0, col=0):
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  headers = [
      {'t': 'File size (bytes)',            'f': 'base10_gb'},
      {'t': 'File path',          'c': 80,  'f': None},
  ]
  # Get the top N and then show the top files for that bin_type
  rank_bins = gdetail(bin_type).get_bins()
  rank_keys = list(rank_bins.keys())
  rank_key_order = sorted(rank_keys, key=lambda x: rank_bins[x]['total'], reverse=True)
  bins = gdetail(top_n_type).get_rank_list()
  if len(bins) == 0:
    worksheet.write(row, col, 'No data')
    return
  
  # Create the special 2 row headers
  start_col = col
  for sid in rank_key_order:
    worksheet.merge_range(row, start_col, row, start_col+1, sid, get_cell_format(document, 'header1'))
    write_column_headers(headers, document, worksheet, row+1, start_col)
    start_col += 3
  row += 2
  
  start_row = row
  for key in rank_key_order:
    user_list = bins[key]
    for i in range(len(user_list) - 1, -1, -1):
      worksheet.write_row(
        row, col,
        [user_list[i][0], user_list[i][1]['filename'], ' '],
      )
      row += 1
    col += 3
    row = start_row

def helper_insert_top_use_by(data, document, bin_type, bin_name, worksheet=None, cfg={}, row=0, col=0):
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  headers = [
      {'t': bin_name,             'c': 60,  'f': None},
      {'t': 'File count',                   'f': 'align_right'},
      {'t': 'File size (bytes)',            'f': 'base10_gb'},
  ]
  bins = gdetail(bin_type).get_bins()
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

def helper_insert_use_by_time(data, document, bin_type, bin_name, worksheet=None, cfg={}, row=0, col=0):
  gbasic = lambda x, y='': data.get('basic', {}).get(x, y)
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  nbase = gconf('number_base', 10)
  headers = [
      {'t': bin_name,             'c': 20,  'f': None},
      {'t': 'File count',         'c': 15,  'f': 'align_right'},
      {'t': 'Total size',         'c': 15,  'f': 'base10_gb'},
      {'t': 'Total size (GB)',    'c': 15,  'f': '2_decimal'},
      {'t': '% of total',         'c': 15,  'f': '2_decimal_pct'},
  ]
  div = float(1000**3)
  total_file_size = float(gbasic('file_size_total'))
  data = gdetail(bin_type)
  time_bins = data.get_bin_config()   # Bins based on time
  size_bins = data.get_bin2_config()  # Bins based on file sizes
  size_hist = data.get_histogram()
  max_size = 0
  
  if total_file_size == 0:
    # Handle case of 0 space used and avoid divide by 0
    total_file_size = 1

  last_bin = None
  worksheet.set_column(col+3, col+len(time_bins), 5)
  for bin in size_bins:
    if bin != 'other':
      title = '<= ' + humanize_number(bin, base=nbase)
    else:
      title = '> ' + humanize_number(last_bin, base=nbase)
    headers.append({
        't': title,
        'c': 12,
        'f': 'base10_gb',
    })
    last_bin = bin

  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  last_bin = None
  for bin in time_bins:
    if bin != 'other':
      title = '<= ' + humanize_time(bin)
    else:
      title = '> ' + humanize_time(last_bin)
    size_data = size_hist[bin][2].get_histogram()
    size_keys = size_hist[bin][2].get_bin_config()
    file_size = size_hist[bin][1]
    worksheet.write_row(
      row, col,
      [
        title,
        size_hist[bin][0],
        file_size,
        file_size/div,
        file_size/total_file_size
      ] + [
        size_data[x][1] for x in size_keys
      ],
    )
    if file_size > max_size:
      max_size = file_size
    row += 1
    last_bin = bin

  chartsheet = document.add_chartsheet('%s chart'%worksheet.get_name())
  data_label_format = CELL_FORMAT_STRINGS['2_decimal']['num_format']
  size_max = get_y_axis_max('file_size', max_size/div)
  chart = document.add_chart({'type': 'column'})
  chart.set_legend({'position': 'bottom'})
  chart.add_series({
      'name': 'Capacity (GB)',
      'categories': [worksheet.get_name(), 1, col, row, col],
      'values': [worksheet.get_name(), 1, col+3, row, col+3],
      'data_labels': {'value': True, 'num_format': data_label_format},
  })
  chart.set_y_axis({
      'name': 'Capacity (GB)',
      'min': 0,
      'max': size_max,
  })
  chart2 = document.add_chart({'type': 'line'})
  chart2.add_series({
      'name': '% of total capacity',
      'categories': [worksheet.get_name(), 1, col, row, col],
      'values': [worksheet.get_name(), 1, col+4, row, col+4],
      'marker': {'type': 'diamond'},
      'y2_axis': True,
  })
  chart2.set_y2_axis({
      'name': '% of total capacity',
      'min': 0,
      'max': 1,
      'num_format': CELL_FORMAT_STRINGS['0_decimal_pct']['num_format'],
  })
  chart.combine(chart2)
  chartsheet.set_chart(chart)

def insert_category_def(data, document, worksheet=None, cfg={}, row=0, col=0):
  headers = [{'t': humanize(x)} for x in HistogramStat.CATEGORIES.keys()]

  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  for key in HistogramStat.CATEGORIES.keys():
    worksheet.write_column(row, col, HistogramStat.CATEGORIES[key])
    col += 1

def insert_directory_details(data, document, worksheet=None, cfg={}, row=0, col=0):
  gbasic = lambda x, y='': data.get('basic', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  depth_range = gbasic('max_dir_depth') + 1
  dataset = [
    {'t': 'Directory details'},
    {'l': 'Average directory depth',        'd': gbasic('average_directory_depth'), 'f': '2_decimal'},
    {'l': 'Average directory width',        'd': gbasic('average_directory_width'), 'f': '2_decimal'},
    {'l': 'Deepest directory level',        'd': gbasic('max_dir_depth')},
    {'l': 'Widest directory',               'd': gbasic('max_dir_width')},
    {'t': 'Directory details at each directory depth'},
    {'l': '',                               'd': [x for x in range(gbasic('max_dir_depth') + 1)],                 'f': 'align_right'},
    {'l': 'Average file size @ depth',      'd': gbasic('average_file_size_per_dir_depth')[0:depth_range],        'f': 'base10_gb'},
    {'l': 'Average file block size @ depth','d': gbasic('average_file_size_block_per_dir_depth')[0:depth_range],  'f': 'base10_gb'},
    {'l': 'Average files @ depth',          'd': gbasic('average_files_per_dir_depth')[0:depth_range],            'f': '2_decimal'},
  ]
  
  worksheet.set_column(col, col, 30)
  worksheet.set_column(col+1, col+depth_range, 14, get_cell_format(document, 'align_right'))
  for x in dataset:
    if x.get('t'):
      worksheet.merge_range(row, col, row, col+1, x['t'], get_cell_format(document, 'header1'))
    else:
      data = x.get('d', '')
      worksheet.write(row, col, x.get('l', ''))
      if isinstance(data, list):
        worksheet.write_row(row, col + 1, data, get_cell_format(document, x.get('f')))
      else:
        worksheet.write(row, col + 1, data, get_cell_format(document, x.get('f')))
    row += 1

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
  total_files = float(gbasic('processed_files'))
  adata = gdetail('hist_file_count_by_atime')
  cdata = gdetail('hist_file_count_by_ctime')
  mdata = gdetail('hist_file_count_by_mtime')
  bins = adata.get_bin_config()
  ahist = adata.get_histogram()
  chist = cdata.get_histogram()
  mhist = mdata.get_histogram()

  if total_files == 0:
    # Handle case of 0 space used and avoid divide by 0
    total_files = 1

  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  last_bin = None
  for bin in bins:
    if bin != 'other':
      title = '<= ' + humanize_time(bin)
    else:
      title = '> ' + humanize_time(last_bin)
    worksheet.write_row(
      row,
      col,
      [
        title,
        ahist[bin][0], ahist[bin][0]/total_files,
        chist[bin][0], chist[bin][0]/total_files,
        mhist[bin][0], mhist[bin][0]/total_files,
      ]
    )
    last_bin = bin
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
  headers = [
      {'t': 'File category',                  'f': 'align_right'},
      {'t': 'File count',                     'f': 'align_right'},
      {'t': 'File size',            'c': 30,  'f': 'base10_gb'},
      {'t': '% of total capacity',  'c': 30,  'f': '2_decimal_pct'},
  ]
  bins = data['detailed']['category'].get_bins()
  keys = list(bins.keys())
  key_order = sorted(keys, key=lambda x: bins[x]['total'], reverse=True)
  total_file_size = float(gbasic('file_size_total'))
  max_row = cfg.get('top_n', float('inf'))

  if total_file_size == 0:
    # Handle case of 0 space used and avoid divide by 0
    total_file_size = 1

  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  for key in key_order:
    worksheet.write_row(
      row,
      col,
      [humanize(key), bins[key]['count'], bins[key]['total'], bins[key]['total']/total_file_size])
    row += 1
    if row > max_row:
      break

def insert_file_extensions_by_size(data, document, worksheet=None, cfg={}, row=0, col=0):
  gbasic = lambda x, y='': data.get('basic', {}).get(x, y)
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  headers = [
      {'t': 'File extension',                 'f': 'align_right'},
      {'t': 'File count',                     'f': 'align_right'},
      {'t': 'File size',            'c': 30,  'f': 'base10_gb'},
      {'t': '% of total capacity',  'c': 30,  'f': '2_decimal_pct'},
      {'t': 'Category',                       'f': 'align_right'},
  ]
  bins = data['detailed']['extensions'].get_bins()
  keys = list(bins.keys())
  key_order = sorted(keys, key=lambda x: bins[x]['total'], reverse=True)
  total_file_size = float(gbasic('file_size_block_total'))
  max_row = cfg.get('top_n', float('inf'))

  if total_file_size == 0:
    # Handle case of 0 space used and avoid divide by 0
    total_file_size = 1

  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  for key in key_order:
    worksheet.write_row(
      row,
      col,
      [
        key or '<No extension>',
        bins[key]['count'],
        bins[key]['total'],
        bins[key]['total']/total_file_size,
        humanize(HistogramStat.get_file_category(key)),
      ]
    )
    row += 1
    if row > max_row:
      break

def insert_file_sizes_histogram(data, document, worksheet=None, cfg={}, row=0, col=0):
  gbasic = lambda x, y='': data.get('basic', {}).get(x, y)
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  nbase = gconf('number_base', 10)
  headers = [
      {'t': 'File size bins',                                   'f': 'align_right'},
      {'t': 'File count',                                       'f': 'group_num1'},
      {'t': 'File size (bytes)',                                'f': 'base10_gb'},
      {'t': '% of total capacity',                    'c': 30,  'f': '2_decimal_pct'},
      {'t': 'File size (GB)',                                   'f': '4_decimal'},
      {'t': 'File count\nBlock boundary',                       'f': 'group_num1'},
      {'t': 'File size (bytes)\nBlock boundary',                'f': 'base10_gb'},
      {'t': '% of total capacity\nBlock boundary',    'c': 30,  'f': '2_decimal_pct'},
      {'t': 'File size (GB)\nBlock boundary',                   'f': '4_decimal'},
  ]
  div = float(1000**3)
  total_file_size = float(gbasic('file_size_total'))
  total_file_size_block = float(gbasic('file_size_block_total'))
  max_size = 0
  max_size_block = 0
  max_file_count = 0
  max_file_count_block = 0
  
  write_column_headers(headers, document, worksheet, row, col)
  row += 1
  file_size_hist = gdetail('hist_file_count_by_size')
  file_size_block_hist = gdetail('hist_file_count_by_block_size')
  bin_desc = file_size_hist.get_bin_config()
  hist1 = file_size_hist.get_histogram()
  hist2 = file_size_block_hist.get_histogram()
  
  if total_file_size == 0:
    # Handle case of 0 space used and avoid divide by 0
    total_file_size = 1
  if total_file_size_block == 0:
    # Handle case of 0 space used and avoid divide by 0
    total_file_size_block = 1
  
  last_key = None
  sorted_keys = sorted(bin_desc, key=lambda x: float('inf') if x == 'other' else x)
  for key in sorted_keys:
    bin = hist1[key]
    bin2 = hist2[key]
    if key != 'other':
      title = '<= ' + humanize_number(key, base=nbase)
    else:
      title = '> ' + humanize_number(last_key, base=nbase)
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
  worksheet.write(row-1, col, title, get_cell_format(document, 'gt_base10_gb'))
  
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
      'name': 'Used capacity (GB)',
      'categories': [worksheet.get_name(), 1, col, row, col],
      'values': [worksheet.get_name(), 1, col+4, row, col+4],
      'data_labels': {'value': True, 'num_format': data_label_format},
  })
  # TODO: Uncomment to merge block based counts
  #chart.add_series({
  #    'name': 'Used capacity (GB) (block)',
  #    'categories': [worksheet.get_name(), 1, col, row, col],
  #    'values': [worksheet.get_name(), 1, col+8, row, col+8],
  #    'data_labels': {'value': False, 'num_format': data_label_format},
  #})
  chart.set_y_axis({
      'name': 'Capacity (GB)',
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
  gother = lambda x, y='': data.get('other', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  dataset = [
    {'t': 'File system summary'},
    {'l': 'Total files scanned',            'd': gbasic('processed_files')},
    {'l': 'Total directories scanned',      'd': gbasic('processed_dirs')},
    {'l': 'Total non leaf directories',     'd': gbasic('parent_dirs_total')},
    {'l': 'Total bytes processed',          'd': gbasic('file_size_total'),         'f': 'base10_gb'},
    {'l': 'Total bytes processed (block)',  'd': gbasic('file_size_block_total'),   'f': 'base10_gb'},
    {'l': 'Symlink files',                  'd': gbasic('symlink_files')},
    {'l': 'Skipped files',                  'd': gbasic('skipped_files')},
    {'l': 'Filtered files',                 'd': gbasic('filtered_files')},
    {'l': 'Filtered directories',           'd': gbasic('filtered_dirs')},
    {'l': 'Max files in a directory',       'd': gbasic('max_files_in_dir')},
    {'l': 'Average file size',              'd': gbasic('average_file_size'),       'f': 'base10_gb'},
    {'l': 'Average file size (block)',      'd': gbasic('average_file_size_block'), 'f': 'base10_gb'},
    {'l': 'Average directory depth',        'd': gbasic('average_directory_depth'), 'f': '2_decimal'},
    {'l': 'Average directory width',        'd': gbasic('average_directory_width'), 'f': '2_decimal'},
    {'l': 'Deepest directory level',        'd': gbasic('max_dir_depth')},
    {'l': 'Widest directory',               'd': gbasic('max_dir_width')},
    {'l': 'Block size (bytes)',             'd': gconf('block_size')},
    {'l': 'Number of worker processes',     'd': gbasic('num_workers')},
    {'l': 'Scanned path(s)',                'd': '\n'.join(gbasic('process_paths', [])), 'h': 15*len(gbasic('process_paths', ['']))},
    {'l': 'Prefix path(s)',                 'd': '\n'.join(gother('prefix_paths', [])),  'h': 15*len(gother('prefix_paths', ['']))},
    {'t': ''},
    {'t': 'Processing summary'},
    {'l': 'Files processed/second',         'd': (gbasic('processed_files')/float(gbasic('time_client_processing'))), 'f': '2_decimal'},
    {'l': 'Number of clients',              'd': gbasic('num_clients')},
    {'l': 'Total number of workers',        'd': gbasic('num_workers')},
    {'l': 'Cumulative client process time', 'd': gbasic('time_client_processing'),  'f': '4_decimal'},
    {'l': 'Cumulative DB insert time',      'd': gbasic('time_db_insert'),          'f': '4_decimal'},
    {'l': 'Cumulative data save time',      'd': gbasic('time_data_save'),          'f': '4_decimal'},
    {'l': 'Cumulative file process time',   'd': gbasic('time_handle_file'),        'f': '4_decimal'},
    {'l': 'Cumulative time file stat',      'd': gbasic('time_stat'),               'f': '4_decimal'},
    {'l': 'Cumulative time SID lookup',     'd': gbasic('time_sid_lookup'),         'f': '4_decimal'},
    {'l': 'Cumulative time stats update',   'd': gbasic('time_stats_update'),       'f': '4_decimal'},
  ]
  
  worksheet.set_column(col, col, 30)
  worksheet.set_column(col+1, col+1, 20, get_cell_format(document, 'align_right'))
  for x in dataset:
    if x.get('t'):
      worksheet.merge_range(row, col, row, col+1, x['t'], get_cell_format(document, 'header1'))
    else:
      data = x.get('d', '')
      worksheet.write(row, col, x.get('l', ''))
      worksheet.write(row, col + 1, data, get_cell_format(document, x.get('f')))
      if x.get('h'):
        worksheet.set_row(row, x.get('h'))
    row += 1

def insert_top_n_files(data, document, worksheet=None, cfg={}, row=0, col=0):
  gdetail = lambda x, y='': data.get('detailed', {}).get(x, y)
  gconf = lambda x, y='': data.get('config', {}).get(x, y)
  headers = [
      {'t': 'File size (bytes)',            'f': 'base10_gb'},
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

def insert_top_n_files_per_gid(data, document, worksheet=None, cfg={}, row=0, col=0):
  helper_insert_top_n_files_per(
    data,
    document,
    bin_type='total_by_gid',
    top_n_type='top_n_file_size_by_gid',
    worksheet=worksheet,
    cfg=cfg, row=row, col=col,
  )

def insert_top_n_files_per_sid(data, document, worksheet=None, cfg={}, row=0, col=0):
  helper_insert_top_n_files_per(
    data,
    document,
    bin_type='total_by_sid',
    top_n_type='top_n_file_size_by_sid',
    worksheet=worksheet,
    cfg=cfg, row=row, col=col,
    )

def insert_top_n_files_per_uid(data, document, worksheet=None, cfg={}, row=0, col=0):
  helper_insert_top_n_files_per(
    data,
    document,
    bin_type='total_by_uid',
    top_n_type='top_n_file_size_by_uid',
    worksheet=worksheet,
    cfg=cfg, row=row, col=col,
  )

def insert_top_use_by_sid(data, document, worksheet=None, cfg={}, row=0, col=0):
  helper_insert_top_use_by(
    data,
    document,
    bin_type='total_by_sid',
    bin_name='User (SID)',
    worksheet=worksheet,
    cfg=cfg, row=row, col=col
  )

def insert_top_use_by_uid(data, document, worksheet=None, cfg={}, row=0, col=0):
  helper_insert_top_use_by(
    data,
    document,
    bin_type='total_by_uid',
    bin_name='User (UID)',
    worksheet=worksheet,
    cfg=cfg, row=row, col=col
  )

def insert_use_by_atime(data, document, worksheet=None, cfg={}, row=0, col=0):
  helper_insert_use_by_time(
    data,
    document,
    bin_type='hist_file_count_by_atime',
    bin_name='Files accessed',
    worksheet=worksheet,
    cfg=cfg, row=row, col=col
  )

def insert_use_by_ctime(data, document, worksheet=None, cfg={}, row=0, col=0):
  helper_insert_use_by_time(
    data,
    document,
    bin_type='hist_file_count_by_atime',
    bin_name='Files created/changed',
    worksheet=worksheet,
    cfg=cfg, row=row, col=col
  )

def insert_use_by_mtime(data, document, worksheet=None, cfg={}, row=0, col=0):
  helper_insert_use_by_time(
    data,
    document,
    bin_type='hist_file_count_by_atime',
    bin_name='Files modified',
    worksheet=worksheet,
    cfg=cfg, row=row, col=col
  )

def export_xlsx(data, file):
  modules = [
    ['Summary',               insert_summary],
    ['Directory details',     insert_directory_details],
    ['File sizes',            insert_file_sizes_histogram],
    ['File ages',             insert_file_age_distribution],
    ['Use by ctime',          insert_use_by_ctime],
    ['Use by atime',          insert_use_by_atime],
    ['Use by mtime',          insert_use_by_mtime],
    ['File extensions',       insert_file_extensions_by_size],
    ['File categories',       insert_file_categories_by_size],
    ['Top N files',           insert_top_n_files],
    ['Top N files per SID',   insert_top_n_files_per_sid],
    ['Top N files per UID',   insert_top_n_files_per_uid],
    #['Top N files per GID',   insert_top_n_files_per_gid],
    ['Use by SID',            insert_top_use_by_sid],
    ['Use by UID',            insert_top_use_by_uid],
    ['Category definition',   insert_category_def],
  ]

  workbook = xlsxwriter.Workbook(file)
  cfg = dict(DEFAULT_CFG)
  for mod in modules:
    try:
      worksheet = workbook.add_worksheet(mod[0] if mod[0] else None)
      mod[1](data, workbook, worksheet, cfg)
    except Exception as e:
      logging.getLogger().error('Unable to fully output worksheet: %s\n%s'%(mod[1], traceback.format_exc()))
  try:
    workbook.close()
  except Exception as e:
    logging.getLogger().critical('Unable to write XLSX file: %s'%file)
