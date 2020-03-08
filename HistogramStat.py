# -*- coding: utf8 -*-
"""
Module description here
"""
__title__ = "HistogramStat"
__version__ = "1.0.0"
__all__ = [
  "get_file_category",
  "norm_func_identity",
  "HashBinCountAndValue",
  "HistogramStat",
  "HistogramStatCountAndValue",
  "HistogramStat2D",
  "RankItems",
  "RankItemsByKey",
]
__author__ = "Andrew Chung <acchung@gmail.com>"
__license__ = "MIT"
__copyright__ = """Copyright 2020 Andrew Chung"""

CATEGORY_APP_EXTENSIONS = [
  'exe',    #
  'com',    #
]

CATEGORY_AUDIO_EXTENSIONS = [
  '3ga',    # 
  'aa',     # *Audible Audio book
  'aac',    # 
  'aif',    # 
  'aiff',   # 
  'ape',    # 
  'au',     # 
  'cda',    # 
  'cdda',   # 
  'm4a',    # MPEG-4 audio
  'mid',    # MIDI
  'midi',   # MIDI
  'mka',    # Matroska Audio
  'mod',    # Amiga Music Module file
  'mp3',    # 
  'mpa',    # 
  'oga',    # Ogg Vorbis audio
  'ra',     # Real Audio
  'ram',    # Real Audio
  'snd',    # Generic sound file
  'wav',    # Generic wave form audio
  'wave',   # Generic wave form audio
  'wma',    # Windows Media Audio
]

CATEGORY_COMPRESSION_EXTENSIONS = [
  '7z',     # 7Zip
  'ace',    # 
  'afa',    # 
  'alz',    # 
  'apk',    # Android PacKage archive
  'ar',     # 
  'arc',    # 
  'ark',    # 
  'arj',    # 
  'b1',     # B1
  'b6z',    # B6z
  'bz2',    # Bzip2
  'bzip2',  # Bzip2
  'cab',    # Cabinet file
  'cb7',    # Comic Book 7Zip Archive
  'cba',    # Comic Book ACE Archive
  'cbt',    # Comic Book TAR Archive
  'cbz',    # Comic Book Zip Archive
  'cdx',    # 
  'cfs',    # *Compact File Set
  'cpio',   # 
  'cpt',    # Compact Pro
  'dar',    # *Disk ARchiver
  'dd',     # *Disk Doubler
  'deb',    # Debian Software Archive
  'dmg',    # Apple disk image
  'ear',    # *Enterprise java ARchive
  'gz',     # GZip
  'gzip',   # GZip
  'jar',    # *Java ARchive
  'lbr',    # 
  'lha',    # 
  'llzh',   # 
  'lz',     # 
  'lzh',    # 
  'lzma',   # 
  'partimg',# PartImage
  'pea',    # PeaZip
  'rar',    # 
  'rpm',    # Redhat Package Manager file
  'rz',     # RZip
  's7z',    # 
  'sfx',    # Self extracing archive
  'shar',   # *SHell ARchive
  'sit',    # StuffIt
  'sitx',   # StuffIt X
  'sz',     #
  'tar',    # *Tape ARchive
  'taz',    # 
  'tbz',    # TAR Bzip
  'tbz2',   # TAR Bzip2
  'tgz',    # TAR Gzip
  'tpz',    # 
  'war',    # Java Web Archive
  'xz',     # 
  'z',      # Zip
  'zip',    # Zip archive
  'zoo',    # 
  'zz',     # 
]

CATEGORY_DATA_EXTENSIONS = [
  # Microsoft Access
  'accdb', 'accdt', 'ade', 'adp',
  'db',     # Generic database file
  'db2',    #
  'db3',    #
  'dbs',    # 
  'json',   # JSON file
  'mdb',    # 
  'sqlite', # SQLite
]

CATEGORY_DOCUMENT_EXTENSIONS = [
  '123',    # Lotus 1-2-3
  '12m',    # Lotus 1-2-3
  '1st',    # Readme file
  'ans',    # ANSI text file
  'asc',    # ASCII text file
  'csv',    # Comma Separated Values
  'doc',    # Microsoft Word Document
  'docm',   # Microsoft Word Open XML Macro Enabled Document
  'docx',   # Microsoft Word Open XML Document
  'fods',   # OpenDocument Flat XML Spreadsheet
  'odm',    # OpenDocument Master Document
  'ods',    # OpenDocument Spreadsheet
  'odt',    # OpenDocument Text Document
  'ots',    # OpenDocument Spreadsheet Template
  'pdf',    # Portable Document Exchange file
  'pot',    # Microsoft PowerPoint template
  'potm',   # Microsoft Open XML Macro Enabled PowerPoint template
  'potx',   # Microsoft Open XML PowerPoint template
  'pps',    # Microsoft PowerPoint slideshow
  'ppsx',   # Microsoft Open XML PowerPoint slideshow
  'ppt',    # Microsoft PowerPoint
  'pptm',   # Microsoft Open XML Macro Enabled PowerPoint 
  'pptx',   # Microsoft Open XML PowerPoint file
  'rtf',    # *Rich Text Format
  'tex',    # LaTeX document
  'text',   # Text file
  'txt',    # Text file
  'wks',    # Lotus 1-2-3
  'wku',    # Lotus 1-2-3
  'wpd',    # WordPerfect document
  'xla',    # Microsoft Excel
  'xlam',   # Microsoft Excel
  'xlm',    # Microsoft Excel Macro
  'xls',    # Microsoft Excel binary format
  'xlsb',   # Microsoft Excel
  'xlsm',   # Microsoft Excel Open XML Macro Enabled Spreadsheet
  'xlsx',   # Microsoft Excel Open XML Spreadsheet
  'xlt',    # Microsoft Excel Template
  'xltm',   # Microsoft Excel Open XML Macro Enabled Template
  'xltx',   # Microsoft Excel Open XML Template
]

CATEGORY_IMAGE_EXTENSIONS = [
  'bmp',    # *BitMaP
  'bpg',    # *Better Portable Graphics
  'cr2',    # Canon RAW format
  'cur',    # *CURsor
  'dcm',    # *Digital Imaging and Communications in medicine image
  'dicom',  # *Digital Imaging and Communications in medicine image
  'dng',    # Generic RAW image format
  'eps',    # *Encapsulated PostScript
  'fit',    # *Flexible Image Transport system
  'fits',   # *Flexible Image Transport System
  'flif',   # *Free Lossless Image Format
  'g3',     # *G3 fax image
  'gif',    # *Graphics Interchange Format
  'ico',    # *ICOn
  'icon',   # Generic icon
  'img',    # IMaGe (Various)
  'jpeg',   # *Joint Pictures Experts Group
  'jpg',    # *Joint Pictures experts Group
  'nef',    # Nikon RAW format
  'pam',    # *Portable Arbitrary Map
  'pbm',    # *Portable Bit Map
  'pcc',    # ZSoft PCX Image
  'pcx',    # ZSoft PCX Image
  'pgm',    # *Portable Gray Map
  'png',    # *Portable Network Graphics
  'pnm',    # *Portable aNy Map
  'pns',    # PNG Stereo
  'ppm',    # *Portable Pixel Map
  'ps',     # *PostScript
  'ras',    # *RASter image file
  'raw',    # Generic RAW image format
  'rs',     # Raster image file
  'svg',    # *Scalable Vector Graphics
  'tga',    # #Truevision Graphics Adapter
  'tif',    # *Tagged Image File format
  'tiff',   # *Tagged Image File Format
  'webp',   #
  'xbm',    # *X BitMap
  'xcf',    # Native GIMP image format
  'xpm',    # *X PixelMap
  'xwd',    # X-Windows Dump
]

CATEGORY_SCRIPT_EXTENSIONS = [
  'bat',    # Batch script
  'bash',   # Bash script
  'cdxml',  # PowerShell script
  'lasso',  # Lasso
  'lua',    # LUA
  # Pearl
  'pl', 'plx', 'pm', 'xs', 't', 'pod',
  # PHP
  'php', 'php3', 'php4', 'php5', 'php7', 'phps', 'php-s', 'phar', 'pht', 'phtml',
  # PowerShell script
  'ps1', 'ps1xml', 'psc1', 'psd1', 'psm1', 'pssc',
  # Python
  'py', 'pyc', 'pyd', 'pyi', 'pyw', 'pyz',
  'rb',     # Ruby
  'sh',     # SHell script
  'tcl',    # TCL
  'tbc',    # TCL
  'vbs',    # Visual Basic
  'zsh',    # ZShell script
]

CATEGORY_SOURCE_CODE_EXTENSIONS = [
  'asm',    # General assembly code
  # C and C++
  'c', 'cc', 'cpp', 'cxx', 'c++', 'h', 'hpp', 'hxx',
  'cs',     # C#
  # Fortran
  'f', 'for', 'f90',
  'go',     # Go
  # Java
  'class', 'java',
  'js',     # Javascript
  # Rust
  'rs', 'rlib',
  'swift',  # Swift
  # Verilog
  'v', 'vh',
  'vhdl',   # VHDL
]

CATEGORY_VIDEO_EXTENSIONS = [
  '264',    # h.264 video file
  '3g2',    # Mobile phone video format
  '3gp',    # Mobile phone video format
  '3gp2',   # Mobile phone video format
  '3gpp',   # Mobile phone video format
  '3gpp2',  # Mobile phone video format
  '60d',    # Wavereader CCTV video file
  'ajp',    # CCTV video file
  'am4',    # Security First CCTV video file
  'amv',    # Modified version of AVI
  'arv',    # Everfocus CCTV video file
  'asd',    # Microsoft advanced streaming format
  'asf',    # *Advanced Systems Format
  'avb',    # Avid bin file format
  'avd',    # Avid video file
  'avi',    # *Audio Video Interleved
  'drc',    # Dirac
  'f4b',    # Flash video
  'f4p',    # Flash video
  'f4v',    # Flash video
  'flv',    # Flash video
  'm2ts',   # *Mpeg-2 Transport Stream
  'm2v',    # MPEG-2
  'm4p',    # MPEG-4 video with DRM
  'm4v',    # MPEG-4 video
  'mbf',    # CCTV video file
  'mkv',    # *MatosKa Video
  'mov',    # QuickTime file format
  'mp2',    # MPEG-1
  'mp4',    # *Mpeg-4
  'mpe',    # MPEG-1
  'mpeg',   # MPEG-1/MPEG-2
  'mpg',    # MPEG-1/MPEG-2
  'mpv',    # MPEG-1
  'mts',    # *Mpeg Transport Stream
  'nsv',    # *Nullsoft Streaming Video
  'ogg',    # *OGG vorbis
  'ogv',    # *OGg vorbis Video
  'ogx',    # *OGg vorbis multipleXed media file
  'pns',    # Pelco PNS CCTV video file
  'qt',     # *QuickTime file format
  'rm',     # *RealMedia
  'rmvb',   # *RealMedia Variable Bitrate
  'sdv',    # *Samsung Digital Video
  'svi',    # Samsung video format
  'tivo',   # TIVO video file
  'tod',    # JVC camcorder video file
  'ts',     # *mpeg-2 Transport Stream
  'vmb',    # CCTV video file
  'vob',    # MPEG based video format for DVDs
  'webm',   # *WebM
  'wmv',    # *Windows Media Video
  'xvid',   # XviD encoded video file
]

CATEGORIES = {
  'application': CATEGORY_APP_EXTENSIONS,
  'audio': CATEGORY_AUDIO_EXTENSIONS,
  'compressed': CATEGORY_COMPRESSION_EXTENSIONS,
  'document': CATEGORY_DOCUMENT_EXTENSIONS,
  'data': CATEGORY_DATA_EXTENSIONS,
  'image': CATEGORY_IMAGE_EXTENSIONS,
  'scripts': CATEGORY_SCRIPT_EXTENSIONS,
  'source_code': CATEGORY_SOURCE_CODE_EXTENSIONS,
  'video': CATEGORY_VIDEO_EXTENSIONS,
}

CATEGORY_MAP = {}

def get_file_category(file_ext, categories=CATEGORIES, notfound='other'):
  global CATEGORY_MAP
  if not CATEGORY_MAP:
    for cat in categories:
      for ext in categories[cat]:
        CATEGORY_MAP[ext] = cat
  return CATEGORY_MAP.get(file_ext, notfound)

def norm_func_identity(x):
  return x

'''
HashBinCountAndValue stores count and total based on a key/bin value
As an example, if you have a key value of 'docx' you can call insert_data
using 'docx' as the key and using the size of a file as the data value.
The result when get_bins is called would be the total size of all files that
have a docx extension.
'''
class HashBinCountAndValue():
  def __init__(self):
    self.bins = {}
    self.keys_cache = []
    self.item_count = 0
    
  def insert_data(self, key, data):
    if not key in self.keys_cache:
      self.bins[key] = [0, 0]
      self.keys_cache.append(key)
    self.bins[key][0] += 1
    self.bins[key][1] += data
    
  def get_bin_config(self):
    if None in self.keys_cache:
      self.keys_cache.remove(None)
      self.keys_cache.sort()
      self.keys_cache.append(None)
    else:
      self.keys_cache.sort()
    return self.keys_cache
    
  def get_bins(self):
    data = {}
    for key in self.get_bin_config():
      data[key] = {'count': self.bins[key][0], 'total': self.bins[key][1]}
    return data
    
  def merge(self, other):
    if not isinstance(other, HashBinCountAndValue):
      raise(TypeError('An object of type HashBinCountAndValue required'))
    other_bins = other.get_bins()
    for key in other_bins:
      if key in self.bins:
        self.bins[key][0] += other_bins[key]['count']
        self.bins[key][1] += other_bins[key]['total']
      else:
        self.bins[key] = [other_bins[key]['count'], other_bins[key]['total']]
    # Combine the keys
    self.keys_cache = list(set().union(self.keys_cache, other.keys_cache))
    self.item_count += other.item_count
    

class HistogramStat():
  def __init__(self, bin_config, cache=10240):
    self.bin_config = bin_config[:]
    self.bin_length = len(self.bin_config)
    self.bins = {}
    self.item_count = 0
    # Variables below should not be merged
    self.cache = [0]*cache
    self.cache_idx = -1
    self.cache_size = cache
    if len(self.bin_config) < 1:
      raise ValueError('bin_config must be a list with at least 1 integer element')
    for bin in self.bin_config + ['other']:
      self.bins[bin] = 0

  def insert_data(self, data):
    # Use a cache array and in place modification to save on memory allocations
    self.cache_idx += 1
    self.cache[self.cache_idx] = data
    self.item_count += 1
    if self.cache_idx >= self.cache_size - 1:
      self.flush()
      
  def get_bin_config(self):
    return self.bin_config[:]

  def get_histogram(self, flush=True):
    if flush:
      self.flush()
    return dict(self.bins)
    
  def get_histogram_array(self, flush=True):
    if flush:
      self.flush()
    hist_array = []
    for k in self.bin_config:
      hist_array.append(self.bins[k])
    hist_array.append(self.bins['other'])
    return hist_array
    
  def __len__(self):
    return self.item_count
    
  def flush(self):
    if self.cache_idx < 0:
      return
    # Sort the cached data first
    # Once sorted, we try and walk the histogram bucket and the cache array
    # only once and update the histogram appropriately.
    # Make use of a lot of local variable caching to avoid array indexing
    l = self.cache[:self.cache_idx + 1]
    l.sort()
    bucket_index = 0
    bucket_edge = self.bin_config[bucket_index]
    incr_cache = 0
    for i in range(0, self.cache_idx + 1):
      cache_item = l[i]
      if cache_item <= bucket_edge:
        incr_cache += 1
      else:
        self.bins[bucket_edge] += incr_cache
        incr_cache = 0
        bucket_index += 1
        while bucket_index < self.bin_length:
          bucket_edge = self.bin_config[bucket_index]
          if cache_item <= bucket_edge:
            incr_cache = 1
            break
          bucket_index += 1
        if bucket_index >= self.bin_length:
          self.bins['other'] += self.cache_idx - i + 1
          break
    if incr_cache != 0:
      self.bins[self.bin_config[bucket_index]] += incr_cache
    self.cache_idx = -1
    
  def merge(self, other):
    if not isinstance(other, HistogramStat):
      raise(TypeError('An object of type HistogramStat required'))
    if set(self.bin_config).difference(other.bin_config):
      raise(ValueError("The 2 histogram stats must have the same bin configuration"))
    self.flush()
    other.flush()
    for k in self.bin_config:
      self.bins[k] += other.bins[k]
    self.item_count += other.item_count
    
class HistogramStatCountAndValue(HistogramStat):
  def __init__(self, bin_config, cache=10240, norm=norm_func_identity):
    super(HistogramStatCountAndValue, self).__init__(bin_config, cache)
    self.value_norm_func = norm
    # Each bin is an array of [item_count, running_total]
    for bin in self.bin_config + ['other']:
      self.bins[bin] = [0, 0]

  def flush(self):
    # Sort the cached data first
    # Once sorted, we try and walk the histogram bucket and the cache array
    # only once and update the histogram appropriately.
    if self.cache_idx < 0:
      return
    l = self.cache[:self.cache_idx + 1]
    l.sort()
    bucket_index = 0
    bucket_edge = self.bin_config[bucket_index]
    incr_cache = 0
    val_cache = 0
    for i in range(0, self.cache_idx + 1):
      cache_item = l[i]
      if cache_item <= bucket_edge:
        incr_cache += 1
        val_cache += self.value_norm_func(cache_item)
      else:
        bin_data = self.bins[bucket_edge]
        bin_data[0] += incr_cache
        bin_data[1] += val_cache
        incr_cache = 0
        val_cache = 0
        bucket_index += 1
        while bucket_index < self.bin_length:
          bucket_edge = self.bin_config[bucket_index]
          if cache_item <= bucket_edge:
            incr_cache = 1
            val_cache = self.value_norm_func(cache_item)
            break
          bucket_index += 1
        if bucket_index >= self.bin_length:
          self.bins['other'][0] += self.cache_idx - i + 1
          sum = 0
          for j in l[i:]:
            sum += self.value_norm_func(j)
          self.bins['other'][1] += sum
          break
    if incr_cache != 0:
      self.bins[self.bin_config[bucket_index]][0] += incr_cache
      self.bins[self.bin_config[bucket_index]][1] += val_cache
    self.cache_idx = -1
  
  def merge(self, other):
    if not isinstance(other, HistogramStatCountAndValue):
      raise(TypeError('An object of type HistogramStatCountAndValue required'))
    if set(self.bin_config).difference(other.bin_config):
      raise(ValueError("The 2 histogram stats must have the same bin configuration"))
    other.flush()
    self.flush()
    for k in self.bin_config:
      self.bins[k][0] += other.bins[k][0]
      self.bins[k][1] += other.bins[k][1]
    self.item_count += other.item_count
    

class HistogramStat2D(HistogramStat):
  def __init__(self, bin_config, bin2_config, cache=10240, norm=norm_func_identity, norm2=norm_func_identity):
    super(HistogramStat2D, self).__init__(bin_config, cache=cache)
    self.bin2_config = bin2_config[:]
    self.value_norm_func = norm
    # Each bin is an array of [item_count, running_total, histogram]
    for bin in self.bin_config + ['other']:
      self.bins[bin] = [0, 0, HistogramStatCountAndValue(self.bin2_config, cache=cache, norm=norm2)]

  def get_bin2_config(self):
    return self.bin2_config[:]

  def get_flattened_histogram(self, flush=True):
    if flush:
      self.flush()
    data = self.get_histogram()
    for key in data:
      data[key][2] = data[key][2].get_histogram()
    return(data)

  def get_histogram_array(self, flush=True):
    hist_array = super(HistogramStat2D, self).get_histogram_array(flush=flush)
    for bin in hist_array:
      bin[2] = bin[2].get_histogram_array()
    return hist_array
   
  def insert_data(self, data, data2):
    # Use a cache array and in place modification to save on memory allocations
    self.cache_idx += 1
    self.cache[self.cache_idx] = [data, data2]
    self.item_count += 1
    if self.cache_idx >= self.cache_size - 1:
      self.flush()
      
  def flush(self):
    # Sort the cached data first
    # Once sorted, we try and walk the histogram bucket and the cache array
    # only once and update the histogram appropriately.
    if self.cache_idx < 0:
      return
    l = self.cache[:self.cache_idx + 1]
    l.sort(key=lambda x:x[0])
    bucket_index = 0
    bucket_edge = self.bin_config[bucket_index]
    incr_cache = 0
    val_cache = 0
    for i in range(0, self.cache_idx + 1):
      cache_item = l[i][0]
      data_value = l[i][1]
      if cache_item <= bucket_edge:
        self.bins[bucket_edge][2].insert_data(data_value)
        incr_cache += 1
        val_cache += data_value
      else:
        bin_data = self.bins[bucket_edge]
        bin_data[0] += incr_cache
        bin_data[1] += val_cache
        incr_cache = 0
        val_cache = 0
        bucket_index += 1
        while bucket_index < self.bin_length:
          bucket_edge = self.bin_config[bucket_index]
          if cache_item <= bucket_edge:
            incr_cache = 1
            val_cache = data_value
            self.bins[bucket_edge][2].insert_data(data_value)
            break
          bucket_index += 1
        if bucket_index >= self.bin_length:
          bin_data = self.bins['other']
          self.bins['other'][0] += (self.cache_idx - i + 1)
          sum = 0
          for j in l[i:]:
            data_value = j[1]
            sum += data_value
            bin_data[2].insert_data(data_value)
          self.bins['other'][1] += sum
          break
    if incr_cache != 0:
      self.bins[self.bin_config[bucket_index]][0] += incr_cache
      self.bins[self.bin_config[bucket_index]][1] += val_cache
    # Force a flush of all the secondary histograms
    for bin in self.bin_config:
      self.bins[bin][2].flush()
    self.cache_idx = -1
  
  def merge(self, other):
    if not isinstance(other, HistogramStat2D):
      raise(TypeError('An object of type HistogramStat2D required'))
    if set(self.bin_config).difference(other.bin_config) or set(self.bin2_config).difference(other.bin2_config):
      raise(ValueError("The 2 histogram stats 2D must have the same bin configuration"))
    self.flush()
    other.flush()
    for k in self.bin_config + ['other']:
      self.bins[k][0] += other.bins[k][0]
      self.bins[k][1] += other.bins[k][1]
      self.bins[k][2].merge(other.bins[k][2])
    self.item_count += other.item_count


class RankItems():
  def __init__(self, max_count=100):
    self.max_count = max_count
    # Each item in the ranked_items list is an array of [rank, data]
    self.ranked_items = [[-1, None]]*max_count
    
  def get_rank_list(self):
    # Filter out any unused entries which are denoted by a -1 for the rank
    start_idx = -1
    for i in self.ranked_items:
      if i[0] != -1:
        start_idx += 1
        break
      else:
        start_idx += 1
    if start_idx == -1:
      return []
    return self.ranked_items[start_idx:]
    
  def insert_data(self, rank, data):
    if rank > self.ranked_items[0][0]:
      self.ranked_items[0] = [rank, data]
      self.ranked_items.sort(key=lambda x: x[0])
    
  def merge(self, other):
    if not isinstance(other, RankItems):
      raise(TypeError('An object of type RankItems required'))
    self.ranked_items.extend(other.ranked_items)
    self.ranked_items.sort(key=lambda x: x[0])
    self.ranked_items = self.ranked_items[-self.max_count:]


class RankItemsByKey():
  def __init__(self, max_count=100):
    self.max_count = max_count
    self.ranked_by_key = {}
    
  def get_rank_list(self):
    data = {}
    for key in self.ranked_by_key:
      data[key] = self.ranked_by_key[key].get_rank_list()
    return data
    
  def insert_data(self, key, rank, data):
    if key not in self.ranked_by_key:
      self.ranked_by_key[key] = RankItems(self.max_count)
    self.ranked_by_key[key].insert_data(rank, data)
    
  def merge(self, other):
    if not isinstance(other, RankItemsByKey):
      raise(TypeError('An object of type RankItemsByKey required'))
    for key in other.ranked_by_key:
      rank_list = self.ranked_by_key.get(key)
      if not rank_list:
        self.ranked_by_key[key] = RankItems(self.max_count)
      self.ranked_by_key[key].merge(other.ranked_by_key[key])
