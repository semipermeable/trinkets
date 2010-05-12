#!/usr/bin/python

import sqlite3
import os
import sys
import re
import collections

if len(sys.argv) < 2:
    sys.stderr.write('usage: %s AddressBook.sqlitedb\n' % sys.argv[0])
    sys.exit(-1)

dbfn = sys.argv[1]

conn = sqlite3.connect(dbfn)
c = conn.cursor()

# TODO: error handling
# TODO: make backups

properties = {
    3: 'phone',
    4: 'email',
}

c.execute('drop table if exists properties')
c.execute('create table properties ("id" INTEGER, "property" TEXT)')

for id, property in properties.iteritems():
    c.execute('insert into properties (id, property) VALUES (?, ?)', (id, property)) 

conn.commit()

c.execute('drop table if exists labelmapping')
c.execute('create table labelmapping ("id" INTEGER, "label" TEXT)')

labels = conn.cursor()
labels.execute('select value from ABMultiValueLabel')
id = 1
for row in labels:
    v = row[0]
    sys.stderr.write("writing labelmapping: %d %s\n" % (id, v))
    c.execute('insert into labelmapping (id, label) VALUES (?, ?)', (id, v))
    id += 1

conn.commit()

c.execute('select First, Last, Organization, properties.property, ABMultiValue.value, labelmapping.label as label from ABPerson, ABMultiValue, labelmapping, properties where ROWID=record_id and ABMultiValue.label=labelmapping.id and ABMultiValue.property = properties.id and ABMultiValue.value not null')

entries = {}
columns = ['first', 'last', 'organization']

for row in c:
    first, last, org, prop, val, rawlabel = row
    key = (first, last, org)
    if key not in entries:
        entry = collections.defaultdict(str)
        entry.update(first=first, last=last)
        if org:
            entry['organization'] = org
        entries[key] = entry
    else:
        entry = entries[key]
    label = re.search(r'\W+(\w+)\W', str(rawlabel)).group(1)
    newcol = str(label + ' ' + prop)
    if newcol not in columns:
        columns.append(newcol)
    entry[newcol] = val
    
c.close()

print ','.join(columns)
for entry in entries.itervalues():
    print ','.join(["%%(%s)s" % x for x in columns]) % entry
