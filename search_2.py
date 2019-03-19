#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 16:05:12 2019

@author: onimas
"""

import urllib.request
import urllib.parse
import sqlite3
from bs4 import BeautifulSoup
import re

ignorewords = {'the': 1, 'of': 1, 'to': 1, 'and': 1, 'a': 1,
               'in': 1, 'is': 1, 'it': 1}

class Crawler:
    def __init__(self, dbname):
        self.con = sqlite3.connect(dbname, timeout=10)
        
    def __del__(self):
        self.con.close()
        
    def dbcommit(self):
        self.con.commit()
        
    def getentryid(self, table, field, value, createnew=True):
        cur = self.con.execute(
            "select rowid from %s where %s='%s'" % (table, field, value))
        res = cur.fetchone()
        if res == None:
            cur = self.con.execute(
                    "insert into %s (%s) values ('%s')" % (table, field, value))
            return cur.lastrowid
        else:
            return res[0]
        
    def addtoindex(self, url, soup):
        if self.isindexed(url):
            return
        print('Indexing %s' + url)
        
        text = self.gettextonly(soup)
        words = self.separatewords(text)
        
        urlid = self.getentryid('urllist', 'url', url)
        
        for i in range(len(words)):
            word = words[i]
            if word in ignorewords:
                continue
            wordid = self.getentryid('wordlist', 'word', word)
            self.con.execute("insert into wordlocation(urlid, wordid, location) \
                             values (%d, %d, %d)" % (urlid, wordid, i))
            
    def gettextonly(self, soup):
        v = soup.string
        if v == None:
            c = soup.contents
            resulttext = ''
            for t in c:
                subtext = self.gettextonly(t)
                resulttext += subtext + '\n'
            return resulttext
        else:
            return v.strip()
        
    def separatewords(self, text):
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s != '']
    
    def isindexed(self, url):
        u = self.con.execute("select rowid from urllist where url='%s'" % url).fetchone()
        if u != None:
            v = self.con.execute("select * from wordlocation where urlid=%d" % u[0]).fetchone()
            if v!= None:
                return True
        return False
    
    def addlinkhref(self, urlFrom, urlTo, linkText):
        words = self.separatewords(linkText)
        fromid = self.getentryid('urllist', 'url', urlFrom)
        toid = self.getentryid('urllist', 'url', urlTo)
        if fromid == toid:
            return
        cur = self.con.execute("insert into link(fromid, toid) values (%d, %d)" \
                               % (fromid, toid))
        linkid = cur.lastrowid
        for word in words:
            if word in ignorewords:
                continue
            wordid = self.getentryid('wordlist', 'word', word)
            self.con.execute("insert into linkwords(linkid, wordid) values (%d, %d)" \
                             % (linkid, wordid))
            
    def crawl(self, pages, depth=2):
        for i in range(depth):
            print('depth %d begins' % i)
            newpages = set()
            for page in pages:
                try:
                    c = urllib.request.urlopen(page)
                except:
                    print('Could not open %s' % page)
                    continue
                soup = BeautifulSoup(c.read(), 'lxml')
                self.addtoindex(page, soup)
                
                links = soup('a')
                for link in links:
                    if ('href' in dict(link.attrs)):
                        url = urllib.parse.urljoin(page, link['href'])
                        if url.find("'") != -1:
                            continue
                        url = url.split('#')[0]
                        if url[0:4] == 'http' and not self.isindexed(url):
                            newpages.add(url)
                        linktext = self.gettextonly(link)
                        self.addlinkhref(page, url, linktext)
                self.dbcommit()
            pages = newpages
            
    def createindextables(self):
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid,wordid,location)')
        self.con.execute('create table link(fromid integer,toid integer)')
        self.con.execute('create table linkwords(wordid,linkid)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index wordurlidx on wordlocation(wordid)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')
        self.dbcommit()
        
    def calculate_pagerank(self, iterations=20):
        self.con.execute('drop table if exists pagerank')
        self.con.execute('create table pagerank(urlid primary key, score)')
        
        self.con.execute('insert into pagerank select rowid, 1.0 from urllist')
        self.dbcommit()
        
        for i in range(iterations):
            print('Iteration %d' % i)
            for (urlid,) in self.con.execute('select rowid from urllist'):
                pr = 15
                
                for (linker,) in self.con.execute(
                        'select distinct fromid from link where toid=%d' % urlid):
                    linkingpr = self.con.execute(
                            'select score from pagerank where urlid=%d' % linker).fetchone()[0]
                    
                    linkingcount = self.con.execute(
                            'select count (*) from link where fromid=%d' % linker).fetchone()[0]
                    pr += 0.85*(linkingpr/linkingcount)
                self.con.execute('update pagerank set score=%f where urlid=%d' % (pr, urlid))
            self.dbcommit()
            


"""
import search_2
test = search_2.Crawler('searchindex.db')
test.calculate_pagerank()
"""

"""
[row for row in test.con.execute('select urllist.url,pagerank.score from pagerank,urllist \
                                    where urllist.rowid=pagerank.urlid order by pagerank.score desc')][0:10]
Out[55]: 
[('http://kiwitobes.com/wiki/Main_Page.html', 2.528516),
 ('http://kiwitobes.com/wiki/Programming_language.html', 1.161464),
 ('http://kiwitobes.com/wiki/Computer_language.html', 1.064252),
 ('http://kiwitobes.com/wiki/C_programming_language.html', 0.542686),
 ('http://kiwitobes.com/wiki/Java_programming_language.html', 0.496406),
 ('http://kiwitobes.com/wiki/Object-oriented_programming.html', 0.427582),
 ('http://kiwitobes.com/wiki/Compiler.html', 0.398397),
 ('http://kiwitobes.com/wiki/C%2B%2B.html', 0.395712),
 ('http://kiwitobes.com/wiki/Operating_system.html', 0.38577),
 ('http://kiwitobes.com/wiki/Microsoft_Windows.html', 0.370058)]
"""