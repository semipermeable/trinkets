import sqlite3
import os
import sys
import re
import collections

dbfn = sys.argv[1]

conn = sqlite3.connect(dbfn)
c = conn.cursor()

# TODO: auto-populate properties
# TODO: auto-number ABMultiValueLabel

c.execute('select First, Last, Organization, properties.property, ABMultiValue.value, ABMultiValueLabel.value as label from ABPerson, ABMultiValue, ABMultiValueLabel,properties where ROWID=record_id and ABMultiValue.label=ABMultiValueLabel.id and ABMultiValue.property = properties.id and ABMultiValue.value not null')

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
