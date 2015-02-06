#!/usr/bin/python -u
# coding=utf-8

import Queue
import threading
import re
import string
import MySQLdb
import time
import datetime
import urllib2
from urllib2 import HTTPError, URLError
import urlparse
from BeautifulSoup import BeautifulSoup
import sys, os, getopt

global LOCAL_DB
global LOCAL_CURSOR
global RUN_IN_BG

def get_time ():
	return time.strftime ('%H:%M', time.localtime (time.time ()))

def store_code_info (code_info):
	global LOCAL_DB
	global LOCAL_CURSOR

	if code_info['numeric_code'] != '' and re.match (r'^[0-9]{3}$', code_info['numeric_code']):
		sql = """INSERT INTO api_country_division_localized_names (division_id, locale, localized_name)
	VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE localized_name=%s"""

		if code_info['zh_CN'] is not None:
			params = (code_info['numeric_code'], 'zh', code_info['zh_CN'], code_info['zh_CN'])
			LOCAL_CURSOR.execute (sql, params)

		if code_info['zh_TW'] is not None:
			params = (code_info['numeric_code'], 'zh_TW', code_info['zh_TW'], code_info['zh_TW'])
			LOCAL_CURSOR.execute (sql, params)

		if code_info['zh_HK'] is not None:
			params = (code_info['numeric_code'], 'zh_HK', code_info['zh_HK'], code_info['zh_HK'])
			LOCAL_CURSOR.execute (sql, params)

		print ("INFO (%s) > Got country code: %s, %s, %s, %s" % (get_time (),
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

def parse_page_content (url, page_content):
	print ("INFO (%s) > Parsing page from %s" % (get_time (), url))
	soup_full = BeautifulSoup (page_content, fromEncoding = "utf-8")

	if soup_full is not None:
	#try:
		content_text = soup_full.find ("div", {"id" : "mw-content-text"})
		if content_text == None:
			print ("INFO (%s) > Content text not found from %s" % (get_time (), url))
			return
		table = content_text.find ("table", {"class" : "wikitable sortable"})
		if table == None:
			print ("INFO (%s) > Table not found from %s" % (get_time (), url))
			return

		all_rows = table.findAll ("tr")
		for row in all_rows:
			all_cells = row.findAll ("td")
			if len (all_cells) < 8:
				continue

			code_info = {}
			code_info['numeric_code'] = ''
			code_info['zh_CN'] = ''
			code_info['zh_TW'] = ''
			code_info['zh_HK'] = ''

			code_info['numeric_code'] = get_text_in_cell (all_cells[2])
			code_info['zh_CN'] = get_text_in_cell (all_cells[5])
			code_info['zh_TW'] = get_text_in_cell (all_cells[6])
			code_info['zh_HK'] = get_text_in_cell (all_cells[7])
			store_code_info (code_info)
	#except:
	#	print ("ERROR (%s) > Parsing page content from %s" % (get_time (), url))

def fetch_page_content (url):
	try:
		request = urllib2.Request (url)
		request.add_header ('User-Agent', 'Mozilla/5.0 (Windows; U; Windows NT 6.1; zh-CN; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6')
		my_opener = urllib2.build_opener ()
		f= my_opener.open (request)
		page_content = f.read().decode ('utf-8')
		print ("INFO (%s) > Fetched page from %s" % (get_time (), url))
		return page_content

	except HTTPError, e:
		if e.code == 404:
			print ("WARNING (%s) > Failed to fetch page content from %s" % (get_time (), url))
		else:
			print ("ERROR (%s) > HTTPError when fetching page content from %s (%d)" % (get_time (), url, e.code))
	except URLError, e:
		print ("ERROR (%s) > URLError when fetching page content from %s (%s)" % (get_time (), url, e.reason))
	except:
		print ("ERROR (%s) > Unknown error when fetching page content from %s" % (get_time (), url))

	return None

def main (lang):
	global LOCAL_DB
	global LOCAL_CURSOR
	global RUN_IN_BG

	start = time.time()

	url = 'http://' + lang + '.wikipedia.org/wiki/ISO_3166-1'
	page_content = fetch_page_content (url)

	if page_content is not None:
		LOCAL_DB = MySQLdb.connect (host="ldb", user="fsen_dev", passwd="db4FSEN-DEV@FMSoft0126", db="fsen_dev",
				charset="utf8")
		LOCAL_CURSOR = LOCAL_DB.cursor ()

		parse_page_content (url, page_content)

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
lang = "zh"
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
		print ("INFO (%s) > Fork #1 failed: %d (%s)" % (get_time (), e.errno, e.strerror))
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
			print ("INFO (%s) > Daemon PID %d" % (get_time (), pid))
			fd = open ("/tmp/fetch_country_code.pid", "w")
			fd.write ("%d" % pid)
			fd.close ()
			sys.exit (0)
	except OSError, e:
		print ("INFO (%s) > Fork #2 failed: %d (%s)" % (get_time (), e.errno, e.strerror))
		sys.exit (1)

main (lang)

