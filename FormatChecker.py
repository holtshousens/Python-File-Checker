import sys
import os
import fnmatch
import xml.etree.ElementTree as ET
import FileChecker


class FormatChecker:
    def __init__(self, path):
        self._path = path
        self._errors = []
        self._description = os.path.join(path, "description.xml")
        if not os.path.exists(self._description):
            raise Exception("Format file does not exist.unable to proceed")
        self._errorFileName = os.path.join(self._path, "errors.txt")
        if os.path.exists(self._errorFileName):
            os.remove(path=self._errorFileName)

    def checkFile(self, mask, fieldCount, skip, child, sep):
        cnt = 0
        threadList = []
        for root, dirs, files in os.walk(self._path):
            for name in files:
                if fnmatch.fnmatch(name, mask):
                    cnt += 1
                    tmp = os.path.join(self._path, name)
                    thread = FileChecker.FileCheckThread(tmp, skip, fieldCount, sep, child)
                    threadList.append(thread)
                    thread.start()

        for thread in threadList:
            thread.join()     
        for thread in threadList:
            i = 0
            for line in thread.errorList:
                self._errors.append(line)
                i += 1
                if i > 100:
                    break              
                                         
    def processErrors(self):
        res = 0
        if len(self._errors):
            with open(self._errorFileName, "w") as ef:
                for line in self._errors:
                    ef.write(line + "\n")
            res = 1
        return res

    def parse(self):
        tree = ET.parse(self._description)
        root = tree.getroot()
        for child in root:
            print("processing " + child.attrib["pattern"])
            if "skipRows" in child.attrib:
                skip = int(child.attrib["skipRows"])
            else:
                skip = 0
            if "separator" in child.attrib:
                sep = child.attrib["separator"]
            else:
                sep = None 
            self.checkFile(child.attrib["pattern"], int(child.attrib["fieldCount"]), skip, child, sep)


def main():
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        path = os.path.realpath(__file__)
        path = os.path.dirname(path)
    fc = FormatChecker(path)
    fc.parse()    
    res = fc.processErrors()
    if res:
        print("Completed with exceptions.Please check error log")
        sys.exit(1)
    print("Completed successfully")
    sys.exit(0)
    
if __name__ == "__main__":
    main()
