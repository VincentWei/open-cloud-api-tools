#!/usr/bin/python -u
# encoding: utf-8

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

def store_lang_info (lang, lang_info):
	global DB_CURSOR

	if lang_info['iso_639_2'] != '':
		iso_639_2 = lang_info['iso_639_2'].split ('/')
		if len (iso_639_2) == 2:
			lang_info['iso_639_2'] = iso_639_2[0]
			lang_info['iso_639_2_t'] = iso_639_2[1]
		else:
			lang_info['iso_639_2_t'] = lang_info['iso_639_2']
	else:
		lang_info['iso_639_2_t'] = ''

	if not re.match (r'^[a-z]{3}$', lang_info['iso_639_2']):
		lang_info['iso_639_2'] = None
	if not re.match (r'^[a-z]{3}$', lang_info['iso_639_2_t']):
		lang_info['iso_639_2_t'] = None

	if len (lang_info['iso_639_3']) > 3:
		lang_info['iso_639_3'] = lang_info['iso_639_3'][0:3]
	if not re.match (r'^[a-z]{3}$', lang_info['iso_639_3']):
		lang_info['iso_639_3'] = None

	if lang == 'zh' and lang_info['iso_639_1'] != '':
		print ("INFO (%s) > Got and stored language: %s, %s, %s, %s (%s, %s, %s, %s)" % (myToolbox.get_time (),
				lang_info['iso_639_1'], lang_info['iso_639_2'], lang_info['iso_639_2_t'], lang_info['iso_639_3'],
				lang_info['self_name'], lang_info['en'], lang_info['zh_CN'], lang_info['zh_TW']))

		sql = """INSERT INTO api_languages (iso_639_1_code, iso_639_2_b_code, iso_639_2_t_code, iso_639_3_code, self_name)
	VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE self_name=%s"""
		params = (lang_info['iso_639_1'], lang_info['iso_639_2'], lang_info['iso_639_2_t'],
				lang_info['iso_639_3'], lang_info['self_name'], lang_info['self_name'])
		if DB_CURSOR is None:
			myToolbox.print_sql (sql, params)
		else:
			DB_CURSOR.execute (sql, params)

		sql = """INSERT INTO api_language_localized_names (iso_639_1_code, locale, localized_name)
	VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE localized_name=%s"""

		params = (lang_info['iso_639_1'], lang_info['iso_639_1'], lang_info['self_name'], lang_info['self_name'])
		if DB_CURSOR is None:
			myToolbox.print_sql (sql, params)
		else:
			DB_CURSOR.execute (sql, params)

		if lang_info['en'] != '':
			params = (lang_info['iso_639_1'], 'en', lang_info['en'], lang_info['en'])
			if DB_CURSOR is None:
				myToolbox.print_sql (sql, params)
			else:
				DB_CURSOR.execute (sql, params)

		if lang_info['zh_CN'] != '':
			params = (lang_info['iso_639_1'], 'zh_CN', lang_info['zh_CN'], lang_info['zh_CN'])
			if DB_CURSOR is None:
				myToolbox.print_sql (sql, params)
			else:
				DB_CURSOR.execute (sql, params)

			name_zh = Converter ('zh-hant').convert(lang_info['zh_CN'])
			params = (lang_info['iso_639_1'], 'zh', name_zh, name_zh)
			if DB_CURSOR is None:
				myToolbox.print_sql (sql, params)
			else:
				DB_CURSOR.execute (sql, params)

		if lang_info['zh_TW'] != '':
			params = (lang_info['iso_639_1'], 'zh_TW', lang_info['zh_TW'], lang_info['zh_TW'])
			if DB_CURSOR is None:
				myToolbox.print_sql (sql, params)
			else:
				DB_CURSOR.execute (sql, params)
	else:
		pass

def guess_code_type (lang_info, text):
	if re.match (r'^[A-z]{2}$', text):
		lang_info ['alpha_2_code'] = text
	elif re.match (r'^[A-z]{3}$', text):
		lang_info ['alpha_3_code'] = text
	elif re.match (r'^[0-9]{3}$', text):
		lang_info ['iso_639_1'] = text
	elif not re.match (r'^ISO 3166-2:[A-Z]{2}$', text):
		lang_info ['iso_name'] = text

def get_text_in_cell (cell):
	return myToolbox.html_to_text (cell.renderContents())

def get_text_in_cell_alt (cell):
	if cell.string is None:
		return cell.contents[0]
	else:
		return cell.string

def parse_td_cells (lang, all_cells):
	lang_info = {}
	lang_info['iso_639_1'] = ''
	lang_info['iso_639_2'] = ''
	lang_info['iso_639_3'] = ''
	lang_info['self_name'] = ''
	lang_info['en'] = ''
	lang_info['zh_CN'] = ''
	lang_info['zh_TW'] = ''
	lang_info['note'] = ''

	if lang == 'en':
		pass
	elif lang == 'zh':
		if len (all_cells) < 8:
			return lang_info
		lang_info['iso_639_1'] = get_text_in_cell (all_cells[0])
		lang_info['iso_639_2'] = get_text_in_cell (all_cells[1])
		lang_info['iso_639_3'] = get_text_in_cell (all_cells[2])
		lang_info['self_name'] = get_text_in_cell (all_cells[3])
		lang_info['en'] = get_text_in_cell (all_cells[4])
		lang_info['zh_CN'] = get_text_in_cell (all_cells[5])
		lang_info['zh_TW'] = get_text_in_cell (all_cells[6])
		lang_info['note'] = get_text_in_cell (all_cells[7])

	return lang_info

def parse_page_content (lang, source_name, page_content):
	print ("INFO (%s) > Parsing contents from %s" % (myToolbox.get_time (), source_name))
	soup_full = BeautifulSoup (page_content, fromEncoding = "utf-8")

	if soup_full is not None:
		content_text = soup_full.find ("div", {"id" : "mw-content-text"})
		if content_text == None:
			print ("INFO (%s) > Content text not found from %s" % (myToolbox.get_time (), source_name))
			return
		table = content_text.find ("table", {"class" : "prettytable + sortable"})
		if table == None:
			print ("INFO (%s) > Table not found from %s" % (myToolbox.get_time (), source_name))
			return

		all_rows = table.findAll ("tr")
		for row in all_rows:
			all_cells = row.findAll ("td")
			lang_info = parse_td_cells (lang, all_cells)
			store_lang_info (lang, lang_info)

def main (lang, cache_dir):
	global DB_CONNECTOR
	global DB_CURSOR

	start = time.time()

	page_content = myToolbox.fetch_wikipedia_page_content (lang, 'ISO_639-1代码表', cache_dir)

	if page_content is not None:
		parse_page_content (lang, 'ISO_639-1代码表', page_content)
	else:
		sys.exit (0)

def usage ():
	print "./fetch_languages.py [--help] [--background] [--lang=<en|zh|ja|...>]"

start = time.time()

DB_CONNECTOR = None
DB_CURSOR = None
lang = "zh"
cache_dir = "data/"

opts, args = getopt.getopt (sys.argv [1:], "hdl:c:", ["help", "enable-database", "with-lang=", "with-cache-dir"])
for op, value in opts:
	if op == "-h" or op == "--help":
		usage ()
		sys.exit (0)
	elif op == "-d" or op == "--enable-database":
		import dbConfig
		DB_CONNECTOR = MySQLdb.connect (host=dbConfig.host, user=dbConfig.user, passwd=dbConfig.passwd, db=dbConfig.name,
				charset="utf8")
		DB_CURSOR = DB_CONNECTOR.cursor ()
	elif op == "-l" or op == "--with-lang":
		lang = value
	elif op == "-c" or op == "--with-cache-dir":
		cache_dir = value

reload(sys)
sys.setdefaultencoding('utf-8')

main (lang, cache_dir)

if DB_CURSOR is not None:
	DB_CURSOR.close ()
	DB_CONNECTOR.commit ()

print "Elapsed Time: %s" % (time.time() - start)
sys.exit (0)

