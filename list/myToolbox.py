# encoding: utf-8
#
# This file is a part of Open Cloud API Project.
#
# Open Cloud API project tries to provide free APIs for internet apps
# to fetch public structured data (such as country list) or some
# common computing services (such as generating QR code).
#
# For more information, please refer to:
#
#		http://www.fullstackengineer.net/zh/project/open-cloud-api-zh
#		http://www.fullstackengineer.net/en/project/open-cloud-api-en
#
# Copyright (C) 2015 WEI Yongming
# <http://www.fullstackengineer.net/zh/engineer/weiyongming>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import traceback
import re
import string
import time
import datetime
import urllib2
from urllib2 import HTTPError, URLError
import codecs
from HTMLParser import HTMLParser

from HTMLParser import HTMLParser
import htmlentitydefs

class HTMLTextExtractor(HTMLParser):
	def __init__(self):
		HTMLParser.__init__(self)
		self.result = [ ]

	def handle_data(self, d):
		self.result.append(d)

	def handle_charref(self, number):
		codepoint = int(number[1:], 16) if number[0] in (u'x', u'X') else int(number)
		self.result.append(unichr(codepoint))

	def handle_entityref(self, name):
		codepoint = htmlentitydefs.name2codepoint[name]
		self.result.append(unichr(codepoint))

	def get_text(self):
		return u''.join(self.result)

def html_to_text(html):
	s = HTMLTextExtractor()
	s.feed(html)
	return s.get_text()

def get_time ():
	return time.strftime ('%H:%M', time.localtime (time.time ()))

def fetch_page_content (lang, country_code, cache_dir = ''):
	if re.match (r'^[A-z]{2}$', country_code):
		cached_filename = cache_dir + 'ISO_3166-2-' + country_code + '-' + lang + '.cache'
		url = 'http://' + lang + '.wikipedia.org/wiki/ISO_3166-2:' + country_code
	else:
		cached_filename = cache_dir + 'ISO_3166-1-' + lang + '.cache'
		url = 'http://' + lang + '.wikipedia.org/wiki/ISO_3166-1'

	fd_cached = None
	try:
		print ("INFO (%s) > Trying to load content from %s" % (get_time (), cached_filename))
		fd_cached = codecs.open (cached_filename, "r", "utf-8")
		page_content = fd_cached.read ()
		return page_content
	except:
		try:
			print ("INFO (%s) > Fetching page from %s" % (get_time (), url))

			request = urllib2.Request (url)
			request.add_header ('User-Agent', 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6')
			my_opener = urllib2.build_opener ()
			f = my_opener.open (request)
			page_content = f.read().decode ('utf-8')

			try:
				fd_page = None
				fd_page = codecs.open (cached_filename, "w", "utf-8")
				fd_page.write (page_content)
			except Exception as e:
				print ("ERROR (%s) > Saving page content from %s (%s)" % (get_time (), url, e.reason))
				return None
			finally:
				if fd_page is not None:
					fd_page.close ()

			print ("INFO (%s) > Fetched page from %s" % (get_time (), url))
			return page_content

		except HTTPError as e:
			if e.code == 404:
				print ("WARNING (%s) > Failed to fetch page content from %s" % (get_time (), url))
			else:
				print ("ERROR (%s) > HTTPError when fetching page content from %s (%d)" % (get_time (), url, e.code))
		except URLError as e:
			print ("ERROR (%s) > URLError when fetching page content from %s (%s)" % (get_time (), url, e.reason))
		except Exception as e:
			print ("ERROR (%s) > Unknown error when fetching page content from %s (%s)" % (get_time (), url, e))
	finally:
		if fd_cached is not None:
			fd_cached.close ()

	return None

def fetch_wikipedia_page_content (lang, wiki_word, cache_dir = ''):

	cached_filename = cache_dir + wiki_word + '.cache'
	url = 'http://' + lang + '.wikipedia.org/wiki/' + wiki_word

	fd_cached = None
	try:
		print ("INFO (%s) > Trying to load content from %s" % (get_time (), cached_filename))
		fd_cached = codecs.open (cached_filename, "r", "utf-8")
		page_content = fd_cached.read ()
		return page_content
	except:
		try:
			print ("INFO (%s) > Fetching page from %s" % (get_time (), url))
			request = urllib2.Request (url)
			request.add_header ('User-Agent', 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6')
			my_opener = urllib2.build_opener ()
			f = my_opener.open (request)
			page_content = f.read().decode ('utf-8')

			try:
				fd_page = None
				fd_page = codecs.open (cached_filename, "w", "utf-8")
				fd_page.write (page_content)
			except Exception as e:
				print ("ERROR (%s) > Saving page content from %s (%s)" % (get_time (), url, e.reason))
				return None
			finally:
				if fd_page is not None:
					fd_page.close ()

			print ("INFO (%s) > Fetched page from %s" % (get_time (), url))
			return page_content

		except HTTPError as e:
			if e.code == 404:
				print ("WARNING (%s) > Failed to fetch page content from %s" % (get_time (), url))
			else:
				print ("ERROR (%s) > HTTPError when fetching page content from %s (%d)" % (get_time (), url, e.code))
		except URLError as e:
			print ("ERROR (%s) > URLError when fetching page content from %s (%s)" % (get_time (), url, e.reason))
		except Exception as e:
			print ("ERROR (%s) > Unknown error when fetching page content from %s (%s)" % (get_time (), url, e))
	finally:
		if fd_cached is not None:
			fd_cached.close ()

	return None

def print_sql (sql, params):
	sql = sql.replace ('%s', "'%s'")
	sql = sql + ';'
	print (sql % params)

