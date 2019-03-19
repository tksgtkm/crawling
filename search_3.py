#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 23:56:04 2019

@author: onimas
"""

import sqlite3

class searcher:
    def __init__(self, dbname):
        self.con = sqlite3.connect(dbname)
        
    def __del__(self):
        self.con.close()
        
    def getmatchrows(self, q):
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []
        
        words = q.split(' ')
        tablenumber = 0
        
        for word in words:
            wordrow = self.con.execute(
                    "select rowid from wordlist where word='%s'" % word).fetchone()
            if wordrow != None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber > 0:
                    tablelist += ','
                    clauselist += ' and '
                    clauselist += 'w%d.urlid=w%d.urlid and ' \
                                    % (tablenumber - 1, tablenumber)
                fieldlist += ',w%d.location' % tablenumber
                tablelist += 'wordlocation w%d' % tablenumber
                clauselist += 'w%d.wordid=%d' % (tablenumber, wordid)
                tablenumber += 1
                
        fullquery = 'select %s from %s where %s' % \
                        (fieldlist, tablelist, clauselist)
        
        print(fullquery)
        
        cur = self.con.execute(fullquery)
        rows = [row for row in cur]
        
        return rows, wordids
    
    """
    # result of test:

    import search_3

    test = search_3.searcher('searchindex.db')

    test.getmatchrows('h')
    
    """
    
    def getscoredlist(self, rows, wordids):
        totalscores = dict([(row[0], 0) for row in rows])
        
        
        weights = [(1.0, self.locationscore(rows)),
                    (1.0, self.frequencyscore(rows)),
                    (1.0, self.pagerankscore(rows)),
                    (1.0, self.distancescore(rows)),
                    (5.0, self.inboundlinkscore(rows))]
        
        
        """
        weights = [(1.0, self.frequencyscore(rows)),
                   (1.0, self.locationscore(rows)),
                   (1.0)]
        """
        
        for (weight, scores) in weights:
            for url in totalscores:
                totalscores[url] += weight * scores[url]
                
        return totalscores
    
    def geturlname(self, id):
        return self.con.execute(
                "select url from urllist where rowid=%d" % id).fetchone()[0]
        
    def query(self, q):
        rows, wordids = self.getmatchrows(q)
        scores = self.getscoredlist(rows, wordids)
        rankedscores = sorted([(score, url) for (url, score) in scores.items()],
                               reverse=1)
        for (score, urlid) in rankedscores[0:10]:
            print('%f\t%s' % (score, self.geturlname(urlid)))
        
    def normalizescores(self, scores, smallisbetter=0):
        vsmall = 0.00001
        if smallisbetter:
            minscore = min(scores.values())
            return dict([(u, float(minscore)/max(vsmall, 1)) for (u, l) in scores.items()])
        else:
            maxscores = max(scores.values())
            if maxscores == 0:
                maxscores = vsmall
            return dict([(u, float(c)/maxscores) for (u, c) in scores.items()])
        
    def frequencyscore(self, rows):
        counts = dict([(row[0], 0) for row in rows])
        for row in rows:
            counts[row[0]] +=1
        return self.normalizescores(counts)
    
    def locationscore(self, rows):
        locations = dict([(row[0], 0) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]:
                locations[row[0]] = loc
        return self.normalizescores(locations, smallisbetter=1)
    
    def distancescore(self, rows):
        if len(rows[0]) < 2:
            return dict([(row[0], 1.0) for row in rows])
        
        mindistance = dict([(row[0], 1000000) for row in rows])
        
        for row in rows:
            dist = sum([abs(row[i] - row[i-1]) for i in range(2, len(row))])
            if dist < mindistance[row[0]]:
                mindistance[row[0]] = dist
        return self.normalizescores(mindistance, smallisbetter=1)
    
    def inboundlinkscore(self, rows):
        uniqueurls = dict([(row[0], 1) for row in rows])
        inboundcount = dict([(u, self.con.execute(
                'select count(*) from link where toid=%d' % u)
                                    .fetchone()[0]) for u in uniqueurls])
        return self.normalizescores(inboundcount)
    
    def pagerankscore(self, rows):
        pageranks = dict([(row[0], self.con.execute(
                'select score from pagerank where urlid=%d' % row[0]).fetchone()[0]) for row in rows])
        maxrank = max(pageranks.values())
        normalizedscores = dict([(u, float(l)/maxrank) for (u, l) in pageranks.items()])
        return normalizedscores
    
    def linktextscore(self, rows, wordids):
        linkscores = dict([(row[0], 0) for row in rows])
        for wordid in wordids:
            cur = self.con.execute(
                    'select link.fromid, link.toid from linkwords, link where wordid=%d and linkwords.linkid=link.rowid' % wordid)
        for (fromid, toid) in cur:
            if toid in linkscores:
                pr = self.con.execute(
                        'select score from pagerank where urlid=%d' % fromid).fetchone()[0]
            linkscores[toid] += pr
        maxscore = max(linkscores.values())
        normalizedscores = dict([(u, float(l)/maxscore) for (u, l) in linkscores.items()])
        return normalizedscores

"""
import search_3

test = search_3.searcher('searchindex2.db')

test.query('http w3c')
select w0.urlid,w0.location,w1.location from wordlocation w0,wordlocation w1 where w0.wordid=11 and w0.urlid=w1.urlid and w1.wordid=4
13.011905       http://kiwitobes.com/wiki/Main_Page.html
8.780478        http://kiwitobes.com/wiki/Programming_language.html
8.311319        http://kiwitobes.com/wiki/C_programming_language.html
8.134866        http://kiwitobes.com/wiki/XSLT.html
8.094307        http://kiwitobes.com/wiki/XSL_Transformations.html
8.087280        http://kiwitobes.com/wiki/Java_programming_language.html
7.868982        http://kiwitobes.com/wiki/Microsoft_Windows.html
7.860560        http://kiwitobes.com/wiki/Object-oriented_programming.html
7.753282        http://kiwitobes.com/wiki/Unix.html
7.693157        http://kiwitobes.com/wiki/Compiler.html
"""