#!/usr/bin/env python

"""
imgDir.py - set up files for OCR processing

This program sets up the files for ODW processing
for the INTIP project. It specifies scheduling based on
day and night processing capacity, and produces a
set of files for carrying out hadoop tasks.

For example:

python imgDir.py -b analisa -p process -u www.server.ex/ind -c /home/analisa

The parameters reflect an internal hadoop network that uses public
desktops in off hours.

-b --base: base for file (INTIP newspapers use unique code for each set)
-p --path: path for resulting files
-u --url: path information for retrieving images
-c --cppath: file path for image lists

(c) Copyright GNU General Public License (GPL)
"""

from array import array
from xml.etree import ElementTree as ET

import glob,json, os, tempfile
import traceback
import Image
import sys,os,re,glob
import urllib
import cStringIO

FORMAT = "jpg"

def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""

def hadoopEntry(imgPath,imgList):
    hadoopStr = "echo \"prep for %s\"\n" % imgList
    hadoopStr += "bash /openils/var/bin/prep\n"
    hadoopStr += "time=$(date +\%k\%M)\n"
    hadoopStr += "if [[ $time -ge $start ]] && [[ $time -le $end ]];then\n"
    hadoopStr += "    num=$nightnum\n"
    hadoopStr += "    cp /openils/var/hadoop/conf/slaves.night /openils/var/hadoop/conf/slaves\n"
    hadoopStr += "else\n"
    hadoopStr += "    num=$daynum\n"
    hadoopStr += "    cp /openils/var/hadoop/conf/slaves.lab /openils/var/hadoop/conf/slaves\n"
    hadoopStr += "fi\n"
    hadoopStr += "/openils/var/hadoop/bin/stop-all.sh &\n"
    hadoopStr += "wait\n"
    hadoopStr += "/openils/var/hadoop/bin/start-all.sh &\n"
    hadoopStr += "wait\n"
    hadoopStr += "sleep 10\n"
    hadoopStr += "/openils/var/hadoop/bin/hadoop dfsadmin -safemode leave\n"
    hadoopStr += "/openils/var/hadoop/bin/hadoop fsck -delete\n"
    hadoopStr += "/openils/var/hadoop/bin/hadoop fsck -delete\n"
    hadoopStr += "echo \"pause to copy %s\"\n" % imgList
    hadoopStr += "sleep 30\n"
    hadoopStr += ("/openils/var/hadoop/bin/hadoop dfs -copyFromLocal %s/%s.txt %s.txt\n" % (imgPath,imgList,imgList))
    hadoopStr += "/openils/var/hadoop/bin/hadoop dfs -ls\n"
    hadoopStr += "echo \"starting hadoop\"\n"
    hadoopStr += ("/openils/var/hadoop/bin/hadoop jar /openils/var/hadoop/contrib/streaming/hadoop-*streaming*.jar -D mapred.map.tasks=$num -D mapred.reduce.tasks=0 -file /home/hduser/stuff/hadoop/ossocr.py -mapper /home/hduser/stuff/hadoop/ossocr.py -input %s.txt -output %s\n" % (imgList,imgList))
    hadoopStr += "sleep 10\n"
    hadoopStr += ("bash /usr/local/bin/trans %s\n" % (imgList))
    return hadoopStr

def extractDate(dtName):
    dtParts = dtName.split("_")
    dtFile = dtParts[2].split(".")
    return dtFile[0] + "_" + dtParts[1]


from optparse import OptionParser

parser = OptionParser(usage="""
%prog [options] image1 

Possible choices are:

--base -- specify base for work
--path -- specify path for finding files
--url -- specify url info
--cppath -- specify hadoop copy path

""")

# options
parser.add_option("-b","--base",help="base for file",default=None)
parser.add_option("-p","--path",help="image path",default=None)
parser.add_option("-u","--url",help="info for URL writing",default=None)
parser.add_option("-c","--cppath",help="path for hadoop copy",default=None)

(options,args) = parser.parse_args()


if options.base and options.path and options.url and options.cppath:
   taskFolder = open(options.path + "/" + options.base + ".sh", 'w')
   taskFolder.write("num=0\n" +
      "start=230\n" +
      "end=630\n" +
      "daynum=15\n" +
      "nightnum=110\n\n")
   listFiles = open(options.path + "/" + options.base + ".dat", 'w')
   rotateFile = open(options.path + "/rot.sh",'w')
   modFile = open(options.path + "/mod.sh",'w')
   sampleFile = open(options.path + "/sample.sh",'w')
   fileList = glob.glob('*' + options.base + '*.txt')
   fileList = sorted(fileList,key=extractDate)
   for name in fileList:
      date_info = find_between(name,options.base + "_",".txt")
      listFiles.write("%s_%s\n" % (options.base,date_info))
      rotateFile.write("echo \"rotating %s\"\n" % (date_info))
      rotateFile.write("mogrify -rotate 90 %s/*.%s\n" % (date_info,FORMAT))
      modFile.write("chmod a+r %s/*.%s\n" % (date_info,FORMAT))
      imgFolder = open(options.path + "/" + options.base + "_" + date_info + ".txt", 'w')
      infile = open(name,"r")
      firstLine = True
      for line in infile:
         if len(line) > 0:
            entries = line.split('/')
            if len(entries) > 1:
               imgUrl = "http://%s/%s/%s/%s" % (options.url,options.base,date_info,entries[1])
               imgFolder.write(imgUrl)
               #check images with first line from each set
               if firstLine:
                  sampleFile.write("wget " + imgUrl)
                  firstLine = False
         
      
      taskFolder.write(hadoopEntry(options.cppath,options.base + "_" + date_info))
      imgFolder.close()
      infile.close()
   taskFolder.close()
   listFiles.close()
   rotateFile.close()
   modFile.close()
   sampleFile.close()

else:
   print "missing parameter(s)"
