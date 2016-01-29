import threading
import csv
import copy
import re
import datetime


class FileCheckThread(threading.Thread):

    def __init__(self, filename, skip, fieldCount, sep, childN):
        threading.Thread.__init__(self)
        self._filename = filename
        self.skip = skip
        self.errorList = []
        self.sep = sep
        self._childNode = copy.deepcopy(childN)
        self.fieldCount = fieldCount

    def convertDate(self, value, format, altFormat):
        try:
            dummy = datetime.datetime.strptime(value, format)
            isError = 0
        except ValueError:
            isError = 1 
        if isError and altFormat is not None:   
            dummy = datetime.datetime.strptime(value, altFormat)
            isError = 0
        if isError:
            raise ValueError

    def checkFieldFormat(self, filename, data, childNode, line):
        try:
            err = []
            for fields in childNode:
                for field in fields:
                    fieldIndex = int(field.attrib["fieldIndex"])-1
                    value = data[fieldIndex]
                    if "pattern" in field.attrib and value and re.match(field.attrib["pattern"], value, re.I) is None:
                        err.append("File name " + filename + ", line number " + str(line) +
                                   ": expected value does not match an agreed format. Field Index is " +
                                   str(fieldIndex+1) + " field value is " + value)
                        continue
                    if "dateFormat" in field.attrib:
                        try:
                            format = field.attrib["dateFormat"]
                            aformat = None
                            if "altFormat" in field.attrib:
                                aformat = field.attrib["altFormat"]
                            self.convertDate(value, format, aformat)
                        except ValueError:
                            s = ""
                            if "sampleDate" in field.attrib:
                                s = field.attrib["sampleDate"]
                            err.append("File name " + filename + ", line number " + str(line) + ": field" +
                                       str(fieldIndex+1) + " doesn't match an agreed  format. Example" + s)
        except KeyError:
            print(filename)
            
        if len(err):
            self.errorList.extend(err)
            
    def run(self):
        i = 0
        with open(self._filename, "r") as gp:
            if self.sep is None:
                self.sep = chr(9)
            reader = csv.reader(gp, delimiter=self.sep)
            for line in reader:
                i += 1
                if i <= self.skip:
                    continue
                nmb = len(line)
                if nmb < self.fieldCount:
                    self.errorList.append("File " + self._filename + " line " + str(i) +
                                          ": number of fields doesn't match template. Actual " +
                                          str(nmb) + " expected " + str(self.fieldCount))
                if len(self._childNode):
                    self.checkFieldFormat(self._filename, line, self._childNode.getchildren(), i)
