#!/usr/bin/python -u
# coding=utf-8

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

global LOCAL_DB
global LOCAL_CURSOR
global RUN_IN_BG

def store_code_info (lang, code_info):
	global LOCAL_DB
	global LOCAL_CURSOR

	if lang == 'en' and code_info['numeric_code'] != '':
		sql = """INSERT INTO api_country_codes (numeric_code, alpha_2_code, alpha_3_code, iso_name)
	VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE iso_name=%s"""
		params = (code_info['numeric_code'], code_info['alpha_2_code'], code_info['alpha_3_code'],
				code_info['iso_name'], code_info['iso_name'])
		LOCAL_CURSOR.execute (sql, params)

		sql = """INSERT INTO api_country_division_localized_names (division_id, locale, localized_name)
	VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE localized_name=%s"""
		params = (code_info['numeric_code'], 'en', code_info['iso_name'], code_info['iso_name'])
		LOCAL_CURSOR.execute (sql, params)

		print ("INFO (%s) > Got and stored country code: %s, %s, %s, %s" % (myToolbox.get_time (),
				code_info['numeric_code'], code_info['alpha_2_code'], code_info['alpha_3_code'],
				code_info['iso_name']))
	elif lang == 'zh' and re.match (r'^[0-9]{3}$', code_info['numeric_code']):
		sql = """INSERT INTO api_country_division_localized_names (division_id, locale, localized_name)
	VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE localized_name=%s"""

		if code_info['zh_CN'] is not None:
			params = (code_info['numeric_code'], 'zh_CN', code_info['zh_CN'], code_info['zh_CN'])
			LOCAL_CURSOR.execute (sql, params)

			name_zh = Converter ('zh-hant').convert(code_info['zh_CN'])
			params = (code_info['numeric_code'], 'zh', name_zh, name_zh)
			LOCAL_CURSOR.execute (sql, params)

		if code_info['zh_TW'] is not None:
			params = (code_info['numeric_code'], 'zh_TW', code_info['zh_TW'], code_info['zh_TW'])
			LOCAL_CURSOR.execute (sql, params)

		if code_info['zh_HK'] is not None:
			params = (code_info['numeric_code'], 'zh_HK', code_info['zh_HK'], code_info['zh_HK'])
			LOCAL_CURSOR.execute (sql, params)

		print ("INFO (%s) > Got country code: %s, %s, %s, %s" % (myToolbox.get_time (),
				code_info['numeric_code'], code_info['zh_CN'], code_info['zh_TW'], code_info['zh_HK']))

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

def main (lang):
	global LOCAL_DB
	global LOCAL_CURSOR
	global RUN_IN_BG

	start = time.time()

	page_content = myToolbox.fetch_page_content (lang, '')

	if page_content is not None:
		LOCAL_DB = MySQLdb.connect (host="ldb", user="fsen_dev", passwd="db4FSEN-DEV@FMSoft0126", db="fsen_dev",
				charset="utf8")
		LOCAL_CURSOR = LOCAL_DB.cursor ()

		parse_page_content (lang, '', page_content)

	else:
		if RUN_IN_BG != 0:
			os.remove ("/tmp/fetch_country_code.pid")
		sys.exit (0)

	LOCAL_CURSOR.close ()
	LOCAL_DB.commit ()

	print "Elapsed Time: %s" % (time.time() - start)
	if RUN_IN_BG != 0:
		os.remove ("/tmp/fetch_country_code.pid")
	sys.exit (0)

def usage ():
	print "./fetch_country_code.py [--help] [--background] [--lang=<en|zh|ja|...>]"

RUN_IN_BG = 0
lang = "en"
opts, args = getopt.getopt (sys.argv [1:], "hbl:", ["help", "background", "lang="])
for op, value in opts:
	if op == "-h" or op == "--help":
		usage ()
		sys.exit (0)
	elif op == "-l" or op == "--lang":
		lang = value
	elif op == "-b" or op == "--background":
		RUN_IN_BG = 1

if __name__ == "__main__" and RUN_IN_BG != 0:
	# do the UNIX double-fork magic, see Stevens' "Advanced
	# Programming in the UNIX Environment" for details (ISBN 0201563177)
	try:
		pid = os.fork ()
		if pid > 0:
			# exit first parent
			sys.exit (0)
	except OSError, e:
		print ("INFO (%s) > Fork #1 failed: %d (%s)" % (myToolbox.get_time (), e.errno, e.strerror))
		sys.exit (1)

	# decouple from parent environment
	os.chdir ("/")
	os.setsid ()
	os.umask (0)

	# do second fork
	try:
		pid = os.fork ()
		if pid > 0:
			# exit from second parent, print eventual PID before
			print ("INFO (%s) > Daemon PID %d" % (myToolbox.get_time (), pid))
			fd = open ("/tmp/fetch_country_code.pid", "w")
			fd.write ("%d" % pid)
			fd.close ()
			sys.exit (0)
	except OSError, e:
		print ("INFO (%s) > Fork #2 failed: %d (%s)" % (myToolbox.get_time (), e.errno, e.strerror))
		sys.exit (1)

main (lang)

