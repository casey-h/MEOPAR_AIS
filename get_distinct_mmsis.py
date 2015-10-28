import os

path = os.path.dirname(os.getcwd() + '\\')
listing = os.listdir(path)
mmsis = []

for f in listing:
    count = 0
    if f.find('.csv') == -1: continue
    print f
    with open(os.path.join(path, f), 'r') as infile:
        c = infile.readline()
        while True:
            c = infile.readline()
            if not c: break
            count += 1
            if count % 10000 == 0:
                print count
            c = c.split(',')[0].strip('"')
            if c not in mmsis:
                mmsis.append(c)
                
with open(os.path.join(path, 'mmsis.csv'), 'w') as outfile:
    for mmsi in mmsis:
        outfile.write('{0}\n'.format(mmsi))