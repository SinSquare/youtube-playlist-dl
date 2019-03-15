#!/usr/bin/env python3

from argparse import ArgumentParser
import atexit
import fnmatch
import hashlib
import json
import os
import random
import re
import sqlite3
import string
import subprocess
import sys
import tempfile
import time


#database handling
class Database:
    #open the database
    #attach exit handler
    def __init__(self, dbFile):
        def exitHandler():
            print("exit db")
            if self.connection:
                self.connection.close()

        atexit.register(exitHandler)
        try:
            self.connection = sqlite3.connect(dbFile)
            self.connection.row_factory = sqlite3.Row
            self.connection.isolation_level = "IMMEDIATE"
            print(sqlite3.version)
        except sqlite3.Error as e:
            self.fatalError(str(e), e)

        self.initiateDatabse()

    #close the database
    def close(self):
        if self.connection:
            self.connection.close()
        self.connection = None

    #initialize the database table
    #does not support table update
    def initiateDatabse(self):
        tableSql = """CREATE TABLE IF NOT EXISTS video_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            youtube_id TEXT NOT NULL,
            file_name TEXT NOT NULL,
            file_hash TEXT NOT NULL,
            position INTEGER NOT NULL,
            insert_date INTEGER NOT NULL,
            CONSTRAINT unique_youtube_id UNIQUE (youtube_id)
            ); """

        try:
            c = self.connection.cursor()
            c.execute(tableSql)
        except sqlite3.Error as e:
            self.fatalError(str(e), e)

    #get data by url
    def getUrlData(self, url):
        try:
            c = self.connection.cursor()
            c.execute("SELECT * FROM video_data WHERE youtube_id=?", (url, ))
            return c.fetchone()
        except sqlite3.Error as e:
            print(e)

    #get data by hash
    def getHashData(self, hash):
        try:
            c = self.connection.cursor()
            c.execute("SELECT * FROM video_data WHERE file_hash=?", (hash, ))
            return c.fetchone()
        except sqlite3.Error as e:
            print(e)

    #get position data - ordered filehash list
    def getPositionData(self):
        try:
            c = self.connection.cursor()
            c.execute(
                "SELECT file_name, file_hash FROM video_data ORDER BY position ASC"
            )
            return c.fetchall()
        except sqlite3.Error as e:
            print(e)

    #update position for url
    def updateUrlPosition(self, url, position):
        data = self.getUrlData(url)
        if data['id'] != None:
            sql = 'UPDATE video_data SET position = position + 1 WHERE position >= ? AND id <> ?'
            try:
                c = self.connection.cursor()
                c.execute(sql, (position, data['id']))
                self.connection.commit()
            except sqlite3.Error as e:
                print(e)
            sql = 'UPDATE video_data SET position = ? WHERE id = ?'
            try:
                c = self.connection.cursor()
                c.execute(sql, (position, data['id']))
                self.connection.commit()
            except sqlite3.Error as e:
                print(e)

    #save data for url
    def saveUrlData(self, url, fileName, position, fileHash):
        if not self.connection:
            self.fatalError("No connection")

        data = self.getUrlData(url)
        _id = None
        if data != None:
            sql = 'UPDATE video_data SET file_name = ?, file_hash = ?, position = ?, insert_date = ? WHERE id = ?'
            try:
                c = self.connection.cursor()
                c.execute(sql, (fileName, fileHash, position, int(time.time()),
                                data['id']))
                self.connection.commit()
                _id = data['id']
            except sqlite3.Error as e:
                print("#1", e)
        else:
            sql = 'INSERT INTO video_data (youtube_id, file_name, file_hash, position, insert_date) VALUES (?, ?, ?, ?, ?)'
            try:
                c = self.connection.cursor()
                c.execute(
                    sql, (url, fileName, fileHash, position, int(time.time())))
                self.connection.commit()
                _id = c.lastrowid
            except sqlite3.Error as e:
                print("#2", e)
            except:
                print("Unexpected error:", sys.exc_info()[0])

        # update position info
        self.updateUrlPosition(url, position)

    # print fatal error
    def fatalError(self, message, previous=None):
        print("SQlite error:", message, previous.__class__.__name__)
        raise Exception(message)


#download a single url and tag the file
class YoutubeUrlDownloader:
    def __init__(self, youtubeDlPath, url, filePath):
        print('Downloading ', url, '...')
        self.ytdl = youtubeDlPath
        self.filePath = filePath
        self.url = url

        self.details = self.getUrlDetails()

        self.downloadUrl()
        self.tagFile()

    def getUrlDetails(self):
        ret = {
            "title": None,
            "artist": None,
            "track": None,
            "islive": False,
        }
        cmd = self.ytdl + " --get-filename -o '#title#%(title)s#artist#%(artist)s#track#%(track)s#islive#%(is_live)r' " + self.url
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        (out, err) = proc.communicate()
        result = re.match(
            r'#title#(?P<title>.*)#artist#(?P<artist>.*)#track#(?P<track>.*)#islive#(?P<islive>.*)',
            out.decode("utf-8"))
        if result:
            if result.group('title') != 'NA':
                ret['title'] = result.group('title')

            if result.group('artist') != 'NA':
                ret['artist'] = result.group('artist')

            if result.group('track') != 'NA':
                ret['track'] = result.group('track')

            if result.group('islive') == 'True':
                ret['islive'] = True

        return ret

    def getFileName(self):
        name = None
        if (self.details['artist'] and self.details['track']):
            name = self.details['artist'] + "-" + self.details['track']
        else:
            name = self.details['title']

        if name == None or name == '':
            return self.url + ".mp3"

        name = ' '.join(name.split())
        name = name.replace(" ", "_")
        name = re.sub(r"[^\w\s\-_]", '', name)

        return name.lower() + ".mp3"

    def tagFile(self):
        cmd = ""
        if self.details["artist"]:
            cmd = cmd + " --artist=\"" + self.details["artist"] + "\""
        if self.details["track"]:
            cmd = cmd + " --song=\"" + self.details["track"] + "\""
        cmd = cmd.strip()
        if cmd:
            print(cmd, self.filePath)
            cmd = "id3tag " + cmd + " " + self.filePath
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
            (out, err) = proc.communicate()

    def downloadUrl(self):
        tmp = tempfile.gettempdir() + "/ytdownloader"
        #path = '.'.join(self.filePath.split('.')[:-1])
        cmd = self.ytdl + " --no-cache-dir -f 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best' --limit-rate 10M --extract-audio --audio-format mp3 --output \"" + tmp + ".%(ext)s\" " + self.url
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        (out, err) = proc.communicate()
        os.system("cp %s %s" % (tmp + ".mp3", self.filePath))


class YoutubePlaylist:
    def __init__(self, youtubeDlPath, url):
        self.ytdl = youtubeDlPath
        self.url = url

    def getUrls(self):
        urls = []
        proc = subprocess.Popen(
            self.ytdl + " --flat-playlist -j " + self.url,
            stdout=subprocess.PIPE,
            shell=True)
        (out, err) = proc.communicate()
        for line in out.splitlines():
            try:
                j = json.loads(line)
                urls.append(j['url'])
            except ValueError:  # includes simplejson.decoder.JSONDecodeError
                print('Decoding JSON has failed', line)
        return urls


class RenameFiles:
    def __init__(self, database, outPath):
        fileReader = FileReader(outPath)
        cnt = 1
        for row in database.getPositionData():
            fileName = row["file_name"]
            fileHash = row["file_hash"]
            newName = "%03d-%s" % (cnt, fileName)
            path = fileReader.fileWithHash(fileHash)

            print(fileName, fileHash, newName, path)
            if path == None:
                print('Could not find file with hash...')
            else:
                os.rename(path, outPath + "/" + newName)
            cnt = cnt + 1

        # save the database backup
        database.close()
        os.system("cp %s %s" % (outPath + "/database.db",
                                outPath + "/database.backup"))


class FileReader:
    def __init__(self, outPath):
        self.hashList = {}
        musicExtensions = [
            "*.3gp", "*.aa", "*.aac", "*.aax", "*.act", "*.aiff", "*.amr",
            "*.ape", "*.au", "*.awb", "*.dct", "*.dss", "*.dvf", "*.flac",
            "*.gsm", "*.iklax", "*.ivs", "*.m4a", "*.m4b", "*.m4p", "*.mmf",
            "*.mp3", "*.mpc", "*.msv", "*.nmf", "*.nsf", "*.ogg", "*.oga",
            "*.mogg", "*.opus", "*.ra", "*.rm", "*.raw", "*.sln", "*.tta",
            "*.vox", "*.wav", "*.wma", "*.wv", "*.webm", "*.8svx"
        ]
        for file in os.listdir(outPath):
            for extension in musicExtensions:
                if fnmatch.fnmatch(file, extension):
                    fileHash = self.fileHash(outPath + "/" + file)
                    if self.hashList.get(fileHash) != None:
                        # duplicated file
                        print("Duplicated file, deleting", file)
                        os.remove(outPath + "/" + file)

                    self.hashList[fileHash] = outPath + "/" + file

    def fileHash(self, path):
        BLOCKSIZE = 65536
        hasher = hashlib.sha1()
        with open(path, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
        return hasher.hexdigest()

    def fileWithHash(self, fileHash):
        return self.hashList.get(fileHash)

    def getList(self):
        return self.hashList

    def printList(self):
        print(self.hashList)


def main(listUrl, outDir, ytdl):
    outPath = os.path.realpath(outDir)
    if not os.path.isdir(outPath):
        raise ValueError(
            "destination does not exists, not a directory or not readeable")

    try:
        database = Database(outPath + '/database.db')
    except:
        print("Could not open database, try restoring the backup...")
        os.system("cp %s %s" % (outPath + "/database.backup",
                                outPath + "/database.db"))
        database = Database(outPath + '/database.db')

    fileReader = FileReader(outPath)
    for hash, file in fileReader.getList().items():
        dbData = database.getHashData(hash)
        if dbData == None:
            print('Unknown file, deleting')
            os.remove(file)

    def exitHandler():
        print("exit rename")
        RenameFiles(database, outPath)

    atexit.register(exitHandler)
    urls = YoutubePlaylist(ytdl, listUrl).getUrls()

    for cnt, url in enumerate(urls):
        print(cnt, url)
        dbData = database.getUrlData(url)
        if dbData != None and "file_hash" in dbData.keys():
            fileHash = dbData["file_hash"]
            if fileReader.fileWithHash(fileHash) != None:
                print('File already downloaded', url)
                database.updateUrlPosition(url, cnt)
                continue
        filePath = outPath + "/" + ''.join(
            random.choices(string.ascii_uppercase + string.digits,
                           k=15)) + ".mp3"

        yt = YoutubeUrlDownloader(ytdl, url, filePath)

        fileName = yt.getFileName()
        fileHash = fileReader.fileHash(filePath)

        database.saveUrlData(url, fileName, cnt, fileHash)

    RenameFiles(database, fileReader, outPath)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "-d",
        "--destination",
        help="the destionation to write the files",
        type=str)
    parser.add_argument("-l", "--list", help="the YouTube list url", type=str)
    parser.add_argument(
        "--ytdl", help="youtube-dl path", type=str, default="youtube-dl")

    args = parser.parse_args()

    try:
        main(args.list, args.destination, args.ytdl)
    except (KeyboardInterrupt, SystemExit):
        print("exit")
    except:
        print("Unexpected error:", sys.exc_info()[0])