#!/usr/bin/env python

"""
xmlDir.py - set up files for OCR processing
- art rhyno <http://projectconifer.ca/>

Hadoop jobs produce files with the prefix "part*"
and need to be sorted out afterwards. This
program produces the base xml list of images
and sets up the script to run the processing.

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
from xml.dom import minidom
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement, Comment, tostring


from datetime import datetime

class newsImg:
    def __init__(self, imgName, width, height):
        self.imgName = imgName
        self.width = width
        self.height = height

def find_after( s, first):
    try:
        start = s.index( first ) + len( first )
        end = len(s)
        return s[start:end]
    except ValueError:
        return ""

def cleanprint(elem):
    # see http://renesd.blogspot.com/2007/05/pretty-print-xml-with-python.html
    utf_string = ElementTree.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(utf_string)
    return reparsed.toprettyxml(indent="  ")

def extractDate(dtName):
    dtParts = dtName.split("_")
    print "return", dtParts[1]
    return dtParts[1]

def xmlDirEntry(haPath,webPath):
    xmlStr = "echo \"prepping %s\"\n" % haPath
    xmlStr += "cat %s/part* > results.txt\n" % haPath
    xmlStr += "cat results.txt | python reducer.py > results2.txt\n"
    xmlStr += "cat results2.txt | sort > page.txt\n"
    xmlStr += "python sortout.py\n"
    xmlStr += "mkdir %s\n" % webPath
    xmlStr += "mv *.box %s\n" % webPath
    xmlStr += "mv *.html %s\n" % webPath
    xmlStr += "mv *.xml %s\n" % webPath
    xmlStr += "mv page.txt %s\n" % webPath
    xmlStr += "rm results*.txt\n"
    return xmlStr

from optparse import OptionParser

parser = OptionParser(usage="""
%prog [options] image1 

Possible choices are:

--base -- specify base for work
--date -- specify date format
--hapath -- specify path for hadoop files
--image -- specify image extension
--webpath -- specify path for apache files
--name -- specify newspaper title

""")

# options
parser.add_option("-b","--base",help="base for file",default=None)
parser.add_option("-d","--date",help="file date info",default=None)
parser.add_option("-a","--hapath",help="path for hadoop files",default=None)
parser.add_option("-i","--image",help="specify image extension",default=None)
parser.add_option("-w","--webpath",help="path for apache files",default=None)
parser.add_option("-n","--newspaper",help="newspaper title",default=None)

(options,args) = parser.parse_args()


if options.base:
   taskFolder = open(options.base + ".sh", 'w')
   fileList = glob.glob('*' + options.base + '_*')
   fileList = sorted(fileList,key=extractDate)
   for name in fileList:
      print "name", name
      xmlfile = open(name + '.xml','wb')
      newspaper = Element('newspaper')
      base = SubElement(newspaper,'base',{'name':options.newspaper,'id':options.base,})
      reel = SubElement(base,'reel',{'name':name,'id':name,})
      if not options.date:
         issue = SubElement(reel,'issue',{'name':name,})

      print "->", (name + '/part*')
      partList = glob.glob(name + '/part*')
      newsImgs = []
      img_no = 0
      for part in partList:
        print "part", part
        filebit = part.split('/')
        last_file = '@@@'
        file = open(part, 'r')
        for line in file:
            try:
                char_cnt, fileinfo, word, x0, y0, x1, y1 = line.split('\t', 7)
                odw_flag = False
                if fileinfo.startswith('/tmp/'):
                    odw_flag = True

                last_slash = fileinfo.rfind('/')
                if last_slash > -1:
                   last_slash+=1
                   fileinfo = fileinfo[last_slash:]

                   if odw_flag:
                       fileinfo = fileinfo.replace('_', '@',1)
                       file_tmp = fileinfo.split('@')
                       file_tmp_num = file_tmp[1]
                       first_dot = file_tmp_num.find('.')
                       if first_dot > -1:
                           file_tmp_num = file_tmp_num[first_dot:]
                           fileinfo = file_tmp[0] + file_tmp_num
                       else:
                           fileinfo = fileinfo.replace('@', '_',1)

                file_parts = fileinfo.split('_')
                filename = file_parts[0]

                width = file_parts[1]
                height = file_parts[2]
            except:
                filename  = ''
            if len(filename) > 0:
                if filename != last_file:
                   #img_no = img_no + 1
                   #img = SubElement(issue,'image',{'w':width,'h':height,'no':str(img_no),'align':'0'})
                   #img.text = filename
                 
                   #print "file", filename
                   #print "width", width
                   #print "height", height
                   newsImgs.append(newsImg(filename,width,height))
                   last_file = filename

      last_file = '@@@'
      issue_str = None

      newsImgs = sorted(newsImgs,key=lambda seg:seg.imgName)
      for newsPage in newsImgs:
         img_no = img_no + 1
         if options.date:
            date_parts = newsPage.imgName.split('-')
            issue_str = date_parts[0]+'-'+date_parts[1]+'-'+date_parts[2]
            issue_date = datetime.strptime(issue_str, options.date)
            issue_str = issue_date.strftime('%B') + ' ' + issue_date.strftime('%d') + ', ' + issue_date.strftime('%Y')
         if issue_str != last_file and options.date:
            issue = SubElement(reel,'issue',{'name':issue_str,})
            img_no = 1
         img = SubElement(issue,'image',{'w':newsPage.width,'h':newsPage.height,'no':str(img_no),'align':'0'})   
         img_name = newsPage.imgName
         if options.image:
            img_parts = img_name.split('.')
            img_name = img_parts[0] + "." + options.image
         img.text = img_name
         last_file = issue_str

      xmlfile.write(cleanprint(newspaper))
      xmlfile.close()

      date_info = find_after(name,options.base + "_")
      taskFolder.write(xmlDirEntry(options.hapath+'/'+name,options.webpath+'/'+date_info))
   taskFolder.close()

else:
   print "missing parameter(s)"
