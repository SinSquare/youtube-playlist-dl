Simple tool to download all the soundtrack from a YouTube playlist.

./code/extractor_v2.py -l LLOxDUzFJlm93QEsjA69jE7g -d /var/www/youtube-playlist-downloader/music

Installation:
- Download this repository
- Install youtube-dl (http://rg3.github.io/youtube-dl/)

Usage:
```
./youtube-playlist-dl.py -l <list id> -d <out path>
```
Options:
```
  --list (-l)          List ID (list=LLOxD... in the URL)

  --destination (-d)   The path to save the soundtracs

  --ytdl               youtube-dl path, defaults to 'youtube-dl'
```