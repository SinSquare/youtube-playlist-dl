#!/usr/bin/env python3

import sys
from argparse import ArgumentParser
import tempfile
import os
import subprocess
import json
import re
import time
import atexit
import sqlite3
import fnmatch


class Database:
    def __init__(self, dbFile):
        try:
            self.connection = sqlite3.connect(dbFile)
            self.connection.row_factory = sqlite3.Row
            print(sqlite3.version)
        except sqlite3.Error as e:
            self.fatalError(str(e), e)

        self.initiateDatabse()

        def exitHandler():
            print("exit db")
            if self.connection:
                self.connection.close()

        atexit.register(exitHandler)

    def close(self):
        if self.connection:
            self.connection.close()

    def initiateDatabse(self):
        tableSql = """CREATE TABLE IF NOT EXISTS video_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            youtube_id TEXT NOT NULL,
            file_name TEXT NOT NULL,
            position INTEGER NOT NULL,
            insert_date INTEGER NOT NULL,
            CONSTRAINT unique_youtube_id UNIQUE (youtube_id)
            ); """

        try:
            c = self.connection.cursor()
            c.execute(tableSql)
        except sqlite3.Error as e:
            self.fatalError(str(e), e)

    def getUrlData(self, url):
        try:
            c = self.connection.cursor()
            c.execute("SELECT * FROM video_data WHERE youtube_id=?", (url, ))
            return c.fetchone()
        except sqlite3.Error as e:
            print(e)

    def getPositionData(self):
        try:
            c = self.connection.cursor()
            c.execute("SELECT file_name FROM video_data ORDER BY position ASC")
            return c.fetchall()
        except sqlite3.Error as e:
            print(e)

    def saveUrlData(self, url, fileName, position):
        if not self.connection:
            self.fatalError("No connection")

        data = self.getUrlData(url)
        _id = None
        if data != None:
            sql = 'UPDATE video_data SET file_name = ?, position = ?, insert_date = ? WHERE id = ?'
            try:
                c = self.connection.cursor()
                c.execute(sql,
                          (fileName, position, int(time.time()), data['id']))
                self.connection.commit()
                _id = data['id']
            except sqlite3.Error as e:
                print(e)
            except:
                print("Unexpected error:", sys.exc_info()[0])
        else:
            sql = 'INSERT INTO video_data (youtube_id, file_name, position, insert_date) VALUES (?, ?, ?, ?)'
            try:
                c = self.connection.cursor()
                c.execute(sql, (url, fileName, position, int(time.time())))
                self.connection.commit()
                _id = c.lastrowid
            except sqlite3.Error as e:
                print(e)
            except:
                print("Unexpected error:", sys.exc_info()[0])

        # update position info
        sql = 'UPDATE video_data SET position = position + 1 WHERE position >= ? AND id <> ?'
        try:
            c = self.connection.cursor()
            c.execute(sql, (position, _id))
            self.connection.commit()
        except sqlite3.Error as e:
            print(e)
        except:
            print("Unexpected error:", sys.exc_info()[0])

    def fatalError(self, message, previous=None):
        raise Exception(message)


class YoutubeUrlDownloader:
    def __init__(self, youtubeDlPath, database, outPath, url, position):
        print('Downloading ', url, '...')
        self.ytdl = youtubeDlPath
        self.db = database
        self.outPath = outPath
        self.url = url
        self.position = position
        ###
        dbData = self.db.getUrlData(url)
        self.fileName = None
        self.filePath = None
        if dbData != None and "file_name" in dbData.keys():
            fName = dbData["file_name"]
            for file in os.listdir(outPath):
                if fnmatch.fnmatch(file, '*' + fName):
                    result = re.match(r'([0-9]{3}\-){1,}(?P<filename>.*)',
                                      file)

                    if result:
                        self.fileName = result.group('filename')
                    else:
                        self.fileName = file
                    print('Found file')
                    break

        if self.fileName == None:
            print("Not found")
            self.details = self.getUrlDetails()
            self.fileName = self.getFileName()
            self.filePath = self.outPath + "/" + self.fileName

            self.downloadUrl()
            self.tagFile()

        self.db.saveUrlData(self.url, self.fileName, self.position)

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
            print(cmd)
            cmd = "id3tag " + cmd + " " + self.filePath
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
            (out, err) = proc.communicate()

    def downloadUrl(self):
        tmp = tempfile.gettempdir() + "/ytdownloader"
        #path = '.'.join(self.filePath.split('.')[:-1])
        cmd = self.ytdl + " -f 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best' --limit-rate 10M '\
        '--extract-audio --audio-format mp3 --output \"" + tmp + ".%(ext)s\" " + self.url
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
        cnt = 1
        for row in database.getPositionData():
            originalName = row["file_name"]
            newname = "%03d-%s" % (cnt, originalName)
            cnt = cnt + 1
            for file in os.listdir(outPath):
                if fnmatch.fnmatch(file, '*' + originalName):
                    os.rename(outPath + "/" + file, outPath + "/" + newname)


def main(listUrl, outDir, ytdl):
    outPath = os.path.realpath(outDir)
    if not os.path.isdir(outPath):
        raise ValueError(
            "destination does not exists, not a directory or not readeable")

    database = Database(outPath + '/database.db')

    def exitHandler():
        print("exit rename")
        RenameFiles(database, outPath)

    atexit.register(exitHandler)

    urls = YoutubePlaylist(ytdl, listUrl).getUrls()

    cnt = 0
    for url in urls:
        yt = YoutubeUrlDownloader(ytdl, database, outPath, url, cnt)
        cnt = cnt + 1
        if cnt % 50 == 0:
            RenameFiles(database, outPath)

    RenameFiles(database, outPath)


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

    main(args.list, args.destination, args.ytdl)