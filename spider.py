#!/usr/bin/python

# all database threading code by Wim Schut, copied from here:
# http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/496799

import sys, os
import urllib2, threading
import urlparse
import sqlite3
import Queue, time, thread
from threading import Thread
import re

threads = [False, False, False, False]

_threadex = thread.allocate_lock()
qthreads = 0
sqlqueue = Queue.Queue()

ConnectCmd = "connect"
SqlCmd = "SQL"
StopCmd = "stop"
class DbCmd:
    def __init__(self, cmd, params=[]):
        self.cmd = cmd
        self.params = params

class DbWrapper(Thread):
    def __init__(self, path, nr):
        Thread.__init__(self)
        self.path = path
        self.nr = nr
    def run(self):
        global qthreads
        con = sqlite3.connect(self.path)
        cur = con.cursor()
        while True:
            s = sqlqueue.get()
            #print "Conn %d -> %s -> %s" % (self.nr, s.cmd, s.params)
            if s.cmd == SqlCmd:
                commitneeded = False
                res = []
#               s.params is a list to bundle statements into a "transaction"
                for sql in s.params:
                    cur.execute(sql[0],sql[1])
                    if not sql[0].upper().startswith("SELECT"): 
                        commitneeded = True
                    for row in cur.fetchall(): res.append(row)
                if commitneeded: con.commit()
                s.resultqueue.put(res)
            else:
                _threadex.acquire()
                qthreads -= 1
                _threadex.release()
#               allow other threads to stop
                sqlqueue.put(s)
                s.resultqueue.put(None)
                break

def execSQL(s):
    if s.cmd == ConnectCmd:
        global qthreads
        _threadex.acquire()
        qthreads += 1
        _threadex.release()
        wrap = DbWrapper(s.params, qthreads)
        wrap.start()
    elif s.cmd == StopCmd:
        s.resultqueue = Queue.Queue()
        sqlqueue.put(s)
#       sleep until all threads are stopped
        while qthreads > 0: time.sleep(0.1)
    else:
        s.resultqueue = Queue.Queue()
        sqlqueue.put(s)
        return s.resultqueue.get()

## if __name__ == "__main__":
##     dbname = "test.db"
##     execSQL(DbCmd(ConnectCmd, dbname))
##     execSQL(DbCmd(SqlCmd, [("create table people (name_last, age integer);",())]))
## #   don't add connections before creating table
##     execSQL(DbCmd(ConnectCmd, dbname))
## #   insert one row
##     execSQL(DbCmd(SqlCmd, [("insert into people (name_last, age) values (?, ?);", ('Smith', 80))]))
## #   insert two rows in one transaction
##     execSQL(DbCmd(SqlCmd, [("insert into people (name_last, age) values (?, ?);", ('Jones', 55)), 
##                            ("insert into people (name_last, age) values (?, ?);", ('Gruns', 25))]))
##     for p in execSQL(DbCmd(SqlCmd, [("select * from people", ())])): print p
##     execSQL(DbCmd(StopCmd))
##     os.remove(dbname)


def log_stdout(msg):
    """Print msg to the screen."""
    print msg

def get_page(url, log):
    """Retrieve URL and return comments, log errors."""
    try:
        page = urllib2.urlopen(url)
        body = page.read()
        page.close()
    except:
        log("Error retrieving: " + url)
        return ''
    return body

def find_links(html):
    """return a list of links in HTML"""
    links = re.compile('<[aA][^>]*[hH][rR][eE][fF]=["\'](.*?)["\'][^>]*>')
    result = links.findall(html)
    print "found %s links" % len(result) 
    return result

def is_download_link(link):
    extensions = ['mp3', 'ogg']
    return link.split('.')[-1].lower() in extensions
    
def should_ignore(link):
    extensions = [
        'jpg', 'jpeg', 'gif', 'png', 'pdf', 'rar', 'zip', 'mov', 'mp4',
        'avi', 'mpg', 'mpeg', 'm4a']
    return link.split('.')[-1].lower() in extensions

def alternate_urls(url):
    alts = [url]
    if '://www.' in url:
        alts.append(url.replace('://www.', '://'))
    else:
        alts.append(url.replace('://', '://www.'))
    a = url.split('/')
    if '%' in url:
        try:
            alts.append('/'.join((a[:-1] + [urllib2.unquote(a[-1])])))
        except:
            pass
    else:
        try:
            alts.append('/'.join((a[:-1] + [urllib2.quote(a[-1])])))
        except:
            pass
    return alts

def create_db():
    """ Set up the database
    """
    connection = sqlite3.connect("urls.db")
    cursor = connection.cursor()
    cursor.execute(
        'CREATE TABLE blog_urls (url VARCHAR(200), updated DATE)')
    cursor.execute(
        'CREATE TABLE file_urls (blog VARCHAR(200), url VARCHAR(300),'
        ' downloaded BOOLEAN)')
    connection.commit()


class Spider:
    """
    The heart of this program, finds all links within a web site.

    run() contains the main loop.
    process_page() retrieves each page and finds the links.
    """

    def __init__(self, max_depth=1, log=None):
        self.max_depth = max_depth
        if log is None:
            #use log_stdout function if no log provided
            self.log = log_stdout
        else:
            self.log = log

    def set_start_url(self, start_url):
        self.URLs = set()
        self.start_url = start_url
        self.URLs.add(start_url)
        self._links_to_process = [(start_url, 0)]
    
    def insert_blog_url(self):
        row = execSQL(DbCmd(
            SqlCmd,
            [("SELECT * FROM blog_urls WHERE url = ?", (self.start_url,))]))
        if row:
            print "blog already added"
            return
        execSQL(DbCmd(
            SqlCmd,
            [("INSERT INTO blog_urls (url) VALUES (?)", (self.start_url,))]))

    def insert_file_url(self, url, downloaded=False):
        row = execSQL(DbCmd(
            SqlCmd,
            [("SELECT * FROM file_urls WHERE url = ?", (url,))]))
        if row:
            print "file already added"
            return
        execSQL(DbCmd(
            SqlCmd,
            [("INSERT INTO file_urls (blog, url, downloaded) VALUES"
                       "(? ,?, ?)", (self.start_url, url, downloaded))]))
    
    def update_blog_url(self, url):
        execSQL(DbCmd(
            SqlCmd,
            [("UPDATE blog_urls SET updated = DATETIME('now')  WHERE url = ?",
            (url,))]))
        
    def url_exists(self, url):
        for alt in alternate_urls(url):
            row = execSQL(DbCmd(SqlCmd,[(
                "SELECT * FROM file_urls WHERE url = ?", (alt,))]))
            if row:
                return True
        return False
         
    def downloaded_file_url(self, url):
        execSQL(DbCmd(SqlCmd,[(
            "UPDATE file_urls SET downloaded = True WHERE url = ?", (url,))]))


    def process_url(self, start_url, thread=0):
        #process list of URLs one at a time
        global threads
        try:
            print "STARTING THREAD %s, maxdepth %s" % (thread, self.max_depth)
            self.update_blog_url(start_url)
            self.set_start_url(start_url)
            while self._links_to_process:
                url, url_depth = self._links_to_process.pop()
                self.log("Retrieving: %s - %s " % (url_depth, url))
                self.process_page(url, url_depth)
            print "STOPPING THREAD %s" % thread
        finally:
            _threadex.acquire()
            threads[thread] = False
            _threadex.release()
        
    def url_in_site(self, link):
        #checks weather the link starts with the base URL
        return link.startswith(self.start_url)

    def process_page(self, url, depth):
        #retrieves page and finds links in it
        html = get_page(url, self.log)
        new_depth = depth + 1
        for link in find_links(html):
            #handle relative links
            try:
                link = urlparse.urljoin(url,link)
            except:
                continue
            if '#' in link:
                link = link.split("#")[0]
            if '?' in link:
                link = link.split("?")[0]
            #make sure this is a new URL within current site
            if link in self.URLs:
                continue
            if should_ignore(link):
                continue
            if is_download_link(link):
                if self.url_exists(link):
                    self.URLs.add(link)
                    continue
                self.log("adding " + link)
                self.URLs.add(link)
                self.insert_file_url(link)
            elif (new_depth <= self.max_depth and self.url_in_site(link)):
                self.URLs.add(link)
                self._links_to_process.append((link, new_depth))
            time.sleep(0.1)
            
def get_blog_urls():
    url_tups = execSQL(
        DbCmd(SqlCmd, [("SELECT url FROM blog_urls ORDER BY updated;", ())]))
    result = [tup[0] for tup in url_tups]
    return result

def download_file(url, thread=0):
    global threads
    print "%s" % url
    if not url.startswith("http://") and not url.startswith("https://"):
        print "weird link"
        execSQL(DbCmd(
            SqlCmd,
            [("UPDATE file_urls SET downloaded = 1 WHERE url = ?" , (url,))]))
        _threadex.acquire()
        threads[thread] = False
        _threadex.release()
        return
    for alt in alternate_urls(url):
        if os.path.exists(alt.split('://')[1]):
            print "already there"
            execSQL(DbCmd(
                SqlCmd,
                [("UPDATE file_urls SET downloaded = 1 WHERE url = ?" , (url,))]))
            _threadex.acquire()
            threads[thread] = False
            _threadex.release()
            return
    try:
        os.popen(
            'curl -s --max-time 600 --connect-timeout 10 --user-agent'
            ' "Mozilla/5.0" --create-dirs --globoff --max-filesize'
            ' 30000000 -o "%s" "%s"'
            % (urllib2.unquote(url.split('://')[1]), url))
    except:
        pass       
    execSQL(DbCmd(
        SqlCmd,
        [("UPDATE file_urls SET downloaded = 1 WHERE url = ?" , (url,))]))
    _threadex.acquire()
    threads[thread] = False
    _threadex.release()
    
def download_files(n=100):
    execSQL(DbCmd(ConnectCmd, "urls.db"))
    rows = execSQL(DbCmd(
        SqlCmd,
        [("SELECT url FROM file_urls WHERE downloaded = ?;", (False,))]))[:n]
    print "%s files to download" % len(rows)
    while rows:
        if False in threads:
            ix = threads.index(False)
            _threadex.acquire()
            threads[ix] = True
            _threadex.release()
            bg = threading.Thread(
                None, download_file, args=(rows.pop(0)[0], ix))
            bg.setDaemon(True)
            bg.start()
    while True in threads:
        time.sleep(10)
        print "waiting"
        pass
    execSQL(DbCmd(StopCmd))
        
def add(url):
    print "adding %s" % url
    connection = sqlite3.connect("urls.db")
    cursor = connection.cursor()
    cursor.execute(
        "SELECT * FROM blog_urls WHERE url = ?", (url,))
    row = cursor.fetchone()
    if row:
        print "blog already added"
        return
    cursor.execute("INSERT INTO blog_urls (url) VALUES"
                   "(?)", (url,))
    connection.commit()

def undo():
    connection = sqlite3.connect("urls.db")
    cursor = connection.cursor()
    undos = open("undo.txt", "r")
    for undo in undos.readlines():
        cursor.execute(
            "UPDATE file_urls SET downloaded = 0 WHERE url = ?",
            ("http://" + undo.strip(),))
        print undo.strip()
        connection.commit()
    undos.close()
    connection.close()

def spider(url=None, depth=1):
    global threads
    execSQL(DbCmd(ConnectCmd, "urls.db"))
    if url:
        spidey = Spider(max_depth=depth)
        spider.process_url(url)
        return
    urls = get_blog_urls()
    spiders = {}
    for ix in range(len(threads)):
        spiders[ix] = Spider(max_depth=depth)
    execSQL(DbCmd(ConnectCmd, "urls.db"))
    while urls:
        if False in threads:
            ix = threads.index(False)
            _threadex.acquire()
            threads[ix] = True
            _threadex.release()
            bg = threading.Thread(
                None, spiders[ix].process_url, args=(urls.pop(0), ix))
            bg.setDaemon(True)
            bg.start()
        time.sleep(0.1)
    while True in threads:
        time.sleep(10)
        print "waiting"
        pass
    execSQL(DbCmd(StopCmd))
            
if __name__ == '__main__':
    if len(sys.argv) < 2:
        spider()
    if sys.argv[1] == 'quick':
        spider(depth=0)
    if sys.argv[1] == 'download':
        if len(sys.argv) > 2:
            download_files(int(sys.argv[2]))
        else:
            download_files()
    if sys.argv[1] == 'createdb':
        create_db()
    if sys.argv[1] == 'add':
        add(sys.argv[2])
    if sys.argv[1] == 'undo':
        undo()
    if sys.argv[1] == 'deep':
        spider(sys.argv[2], 5)

