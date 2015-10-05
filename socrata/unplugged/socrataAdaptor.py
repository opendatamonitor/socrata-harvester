import urllib2
import sys
import getopt
from datetime import datetime

import amara
from amara import tree

import logging
log = logging.getLogger('socrata')


class socrataAdaptor(object):
    '''
    Provides APIs on top of a Socrata instance to pull in the list of dataset
    IDs along with APIs to load the metadata for the actual datasets.
    '''

    def loadUrl(self, url):
        response = urllib2.urlopen(url)
        html = response.read()
        return amara.parse(html)

    def listDatasetIds(self, url):
        doc = self.loadUrl(url)
        idElements = doc.xml_select(u'rdf:RDF/dcat:Dataset/dcterm:identifier')
        return map(lambda i: unicode(i.xml_children[0]), idElements)

    def convertViewUrl(self, baseUrl, path):
        response = urllib2.urlopen("%s/%s" % (baseUrl, path))
        content = response.read()
        return self.convertViewXml(baseUrl, content)

    def convertViewXml(self, objectId, baseUrl, content):
        retVal = {}
        doc = amara.parse(content)

        log.debug(content)

        setXmlAttribute(retVal, u'maintainer',
                        xmlAtrribute(doc, 'view/owner/@displayName'))
        setXmlAttribute(retVal, u'id',
                        xmlAtrribute(doc, 'view/@id'))
        setXmlAttribute(retVal, u'metadata_created',
                        toDateString(xmlAtrribute(doc, 'view/@createdAt')))
        setXmlAttribute(retVal, u'metadata_modified',
                        toDateString(xmlAtrribute(doc,
                                     'view/@viewLastModified')))
        setXmlAttribute(retVal, u'author',
                        xmlAtrribute(doc, 'view/tableAuthor/@displayName'))
        setXmlAttribute(retVal, u'state', u'active')
        setXmlAttribute(retVal, u'license_id',
                        xmlAtrribute(doc, 'view/@licenseId'))
        setXmlAttribute(retVal, u'license',
                        xmlAtrribute(doc, 'view/license/@name'))
        setXmlAttribute(retVal, u'license_title',
                        xmlAtrribute(doc, 'view/license/@name'))
        setXmlAttribute(retVal, u'license_url',
                        xmlAtrribute(doc, 'view/license/@termsLink'))
        setXmlAttribute(retVal, u'tags',
                        xmlElementList(doc, 'view/tags/tags'))
        setXmlAttribute(retVal, u'category',
                        xmlAtrribute(doc, 'view/@category'))
        retVal[u'url'] = "%s/resource/%s" % (baseUrl, retVal[u'id'])

        extras = {}

        addExtras(extras, doc.xml_select('view/metadata/custom_fields/*'))

        retVal[u'extras'] = extras

        if u'category' in retVal:
            if u'tags' in retVal:
                retVal[u'tags'].append(retVal[u'category'])
            else:
                retVal[u'tags'] = [retVal[u'category']]

        replacements = {
            ':': '-',
            '\\': '-',
            '/': '-',
            ' ': '-',
            '(': '_',
            ')': '_',
            '[': '_',
            ']': '_'
        }
        name = xmlAtrribute(doc, 'view/@name')
        for k, v in replacements.iteritems():
            name = name.replace(k, v)
        name = name.lower()

        log.debug(name)
        setXmlAttribute(retVal, u'name', name)
        setXmlAttribute(retVal, u'isopen', True)
        setXmlAttribute(retVal, u'notes_rendered',
                        xmlAtrribute(doc, 'view/@description'))
        setXmlAttribute(retVal, u'title', xmlAtrribute(doc, 'view/@name'))

        html = {u'description': xmlAtrribute(doc, 'view/@description'),
                u'metadata_created': retVal[u'metadata_created'],
                u'metadata_modified': retVal[u'metadata_modified']}
        json = html.copy()
        csv = html.copy()
        xls = html.copy()
        xlsx = html.copy()

        html[u'mimetype'] = "text/html"
        json[u'mimetype'] = "application/json"
        xlsx[u'mimetype'] = \
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        xls[u'mimetype'] = "application/excel"
        csv[u'mimetype'] = "text/csv"

        html[u'format'] = "html"
        json[u'format'] = "json"
        xlsx[u'format'] = "xlsx"
        xls[u'format'] = "xls"
        csv[u'format'] = "csv"

        html[u'name'] = "%s.%s" % (name, "html")
        json[u'name'] = "%s.%s" % (name, "json")
        xlsx[u'name'] = "%s.%s" % (name, "xlsx")
        xls[u'name'] = "%s.%s" % (name, "xls")
        csv[u'name'] = "%s.%s" % (name, "csv")

        html[u'url'] = "%s/resource/%s" % (baseUrl, retVal[u'id'])
        json[u'url'] = "%s/api/views/%s/rows.%s?accessType=DOWNLOAD" %\
            (baseUrl, retVal[u'id'], "json")
        csv[u'url'] = "%s/api/views/%s/rows.%s?accessType=DOWNLOAD" %\
            (baseUrl, retVal[u'id'], "csv")
        xls[u'url'] = "%s/api/views/%s/rows.%s?accessType=DOWNLOAD" %\
            (baseUrl, retVal[u'id'], "xls")
        xlsx[u'url'] = "%s/api/views/%s/rows.%s?accessType=DOWNLOAD" %\
            (baseUrl, retVal[u'id'], "xlsx")

        resources = [html, csv]
        retVal[u'resources'] = resources
        log.debug('e')
        return retVal


def setXmlAttribute(obj, attrName, val):
    if val:
        obj[attrName] = val


def xmlAtrribute(doc, path):
    xmlNodeSet = doc.xml_select(path)
    if xmlNodeSet.count(xmlNodeSet) > 0:
        return unicode(xmlNodeSet[0].xml_value)

    return None


def xmlElement(doc, path):
    xmlNodeSet = doc.xml_select(path)
    if xmlNodeSet.count(xmlNodeSet) > 0:
        return unicode(xmlNodeSet[0].xml_children[0])

    return None


def xmlElementList(doc, path):
    xmlNodeSet = doc.xml_select(path)
    log.debug(xmlNodeSet)

    if xmlNodeSet.count(xmlNodeSet) > 0:
        log.debug(xmlNodeSet)
        log.debug(map(lambda x: unicode(x.xml_children[0]), xmlNodeSet))
        return map(lambda x: unicode(x.xml_children[0]), xmlNodeSet)

    return None


def addExtras(obj, xmlNodeSet):

    for node in xmlNodeSet:
        nodeName = node.xml_name[1]
        for attrName in node.xml_attributes:
            print attrName
            attr = node.xml_attributes.getnode(attrName[0], attrName[1])
            if attrName[1] == "value":
                obj[nodeName] = unicode(attr.xml_value)
            else:
                obj["%s:%s" % (nodeName, attrName[1])] = \
                    unicode(attr.xml_value)


def toDateString(millisString):
    if millisString:
        millis = long(millisString)
        return datetime.fromtimestamp(millis).isoformat()

    return None


def main():
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    # process options
    for o, a in opts:
        if o in ("-h", "--help"):
            print __doc__
            sys.exit(0)
        # process arguments
    x = socrataAdaptor()
    for arg in args:
        # print x.listDatasetIds(arg)
        print x.convertViewUrl(arg)

if __name__ == "__main__":
    main()
