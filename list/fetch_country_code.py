#!/usr/bin/python -u
# coding=utf-8
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

import Queue
import threading
import re
import string
import MySQLdb
import time
import datetime
from BeautifulSoup import BeautifulSoup
import sys, os, getopt
from langconv import Converter
import myToolbox

global DB_CONNECTOR
global DB_CURSOR

def store_code_info (lang, code_info):
	global DB_CURSOR

	if lang == 'en' and code_info['numeric_code'] != '':
		sql = """INSERT INTO api_country_codes (numeric_code, alpha_2_code, alpha_3_code, iso_name)
	VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE iso_name=%s"""
		params = (code_info['numeric_code'], code_info['alpha_2_code'], code_info['alpha_3_code'],
				code_info['iso_name'], code_info['iso_name'])
		if DB_CURSOR is None:
			myToolbox.print_sql (sql, params)
		else:
			DB_CURSOR.execute (sql, params)

		sql = """INSERT INTO api_country_division_localized_names (division_id, locale, localized_name)
	VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE localized_name=%s"""
		params = (code_info['numeric_code'], 'en', code_info['iso_name'], code_info['iso_name'])
		if DB_CURSOR is None:
			myToolbox.print_sql (sql, params)
		else:
			DB_CURSOR.execute (sql, params)

	elif lang == 'zh' and re.match (r'^[0-9]{3}$', code_info['numeric_code']):
		sql = """INSERT INTO api_country_division_localized_names (division_id, locale, localized_name)
	VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE localized_name=%s"""

		if code_info['zh_CN'] is not None:
			params = (code_info['numeric_code'], 'zh_CN', code_info['zh_CN'], code_info['zh_CN'])
			if DB_CURSOR is None:
				myToolbox.print_sql (sql, params)
			else:
				DB_CURSOR.execute (sql, params)

			name_zh = Converter ('zh-hant').convert(code_info['zh_CN'])
			params = (code_info['numeric_code'], 'zh', name_zh, name_zh)
			DB_CURSOR.execute (sql, params)

		if code_info['zh_TW'] is not None:
			params = (code_info['numeric_code'], 'zh_TW', code_info['zh_TW'], code_info['zh_TW'])
			if DB_CURSOR is None:
				myToolbox.print_sql (sql, params)
			else:
				DB_CURSOR.execute (sql, params)

		if code_info['zh_HK'] is not None:
			params = (code_info['numeric_code'], 'zh_HK', code_info['zh_HK'], code_info['zh_HK'])
			if DB_CURSOR is None:
				myToolbox.print_sql (sql, params)
			else:
				DB_CURSOR.execute (sql, params)

def guess_code_type (code_info, text):
	if re.match (r'^[A-z]{2}$', text):
		code_info ['alpha_2_code'] = text
	elif re.match (r'^[A-z]{3}$', text):
		code_info ['alpha_3_code'] = text
	elif re.match (r'^[0-9]{3}$', text):
		code_info ['numeric_code'] = text
	elif not re.match (r'^ISO 3166-2:[A-Z]{2}$', text):
		code_info ['iso_name'] = text

def get_text_in_cell (cell):
	if cell.span is not None:
		text = cell.span.string
	elif cell.a is not None:
		text = cell.a.string
	else:
		text = cell.string
	return text


def parse_td_cells (lang, all_cells):
	code_info = {}
	code_info['numeric_code'] = ''
	if lang == 'en':
		for cell in all_cells:
			sort_text = cell.find ("span", {"class" : "sorttext"})
			if sort_text is not None:
				text = sort_text.a.string
			elif cell.span is not None:
				text = cell.span.string
			elif cell.a is not None:
				text = cell.a.string
			else:
				text = cell.string
			guess_code_type (code_info, text)
	elif lang == 'zh':
		if len (all_cells) < 8:
			return code_info
		code_info['numeric_code'] = get_text_in_cell (all_cells[2])
		code_info['zh_CN'] = get_text_in_cell (all_cells[5])
		code_info['zh_TW'] = get_text_in_cell (all_cells[6])
		code_info['zh_HK'] = get_text_in_cell (all_cells[7])

	return code_info

def parse_page_content (lang, country_code, page_content):
	if re.match (r'^[A-z]{2}$', country_code):
		source_name = 'ISO_3166-2:' + country_code + ' in ' + lang
	else:
		source_name = 'ISO_3166-1 in ' + lang

	print ("INFO (%s) > Parsing contents from %s" % (myToolbox.get_time (), source_name))
	soup_full = BeautifulSoup (page_content, fromEncoding = "utf-8")

	if soup_full is not None:
		content_text = soup_full.find ("div", {"id" : "mw-content-text"})
		if content_text == None:
			print ("INFO (%s) > Content text not found from %s" % (myToolbox.get_time (), source_name))
			return
		table = content_text.find ("table", {"class" : "wikitable sortable"})
		if table == None:
			print ("INFO (%s) > Table not found from %s" % (myToolbox.get_time (), source_name))
			return

		all_rows = table.findAll ("tr")
		for row in all_rows:
			all_cells = row.findAll ("td")
			code_info = parse_td_cells (lang, all_cells)
			store_code_info (lang, code_info)

def main (lang, cache_dir):
	page_content = myToolbox.fetch_page_content (lang, '', cache_dir)

	if page_content is not None:
		parse_page_content (lang, '', page_content)
	else:
		sys.exit (0)

def usage ():
	print "./fetch_country_code.py [--help | -h] [--enable-database | -d] [--with-lang=<en|zh|...> | -l <en|zh|...>]"
	print "    [--with-cache-dir=<cache_dir> | -c <cache_dir>]"
	print ""
	print "    Please specify your database settings in pvDbConfig.py when you use --enable-database option."

start = time.time()

DB_CONNECTOR = None
DB_CURSOR = None

lang = "en"
cache_dir = "data/"

opts, args = getopt.getopt (sys.argv [1:], "hdl:c:", ["help", "enable-database", "with-lang=", "with-cache-dir"])
for op, value in opts:
	if op == "-h" or op == "--help":
		usage ()
		sys.exit (0)
	elif op == "-d" or op == "--enable-database":
		import pvDbConfig
		DB_CONNECTOR = MySQLdb.connect (host=pvDbConfig.host, user=pvDbConfig.user, passwd=pvDbConfig.passwd, db=pvDbConfig.name,
				charset="utf8")
		DB_CURSOR = DB_CONNECTOR.cursor ()
	elif op == "-l" or op == "--with-lang":
		lang = value
	elif op == "-c" or op == "--with-cache-dir":
		cache_dir = value

main (lang, cache_dir)

if DB_CURSOR is not None:
	DB_CURSOR.close ()
	DB_CONNECTOR.commit ()

print "Elapsed Time: %s" % (time.time() - start)
sys.exit (0)

