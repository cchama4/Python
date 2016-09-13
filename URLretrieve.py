# Note - this code must run in Python 2.x and you must download
# http://www.pythonlearn.com/code/BeautifulSoup.py
# Into the same folder as this program

import urllib, sys
from BeautifulSoup import *

flag=0
def retrieve(urlArg):
	html = urllib.urlopen(urlArg).read()
	soup = BeautifulSoup(html)

# Retrieve all of the anchor tags
	tags = soup('a')
	posn=0
	for tag in tags:
		s=tag.get('href', None)
		#print s
		posn=posn+1
		if(posn<int(position)):
			continue
		else:
			global flag
			flag=flag+1
			if(flag==int(count)):
				print s
				sys.exit(0)
			retrieve(s)
url = raw_input('Enter - ')
count=raw_input('Enter count - ')
position=raw_input('Enter position - ')
retrieve(url)