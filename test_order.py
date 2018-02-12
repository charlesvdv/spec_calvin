#!/usr/bin/python3
import sys
import re

def main():
    filenames = sys.argv[1:]
    filenames.sort(key=natural_sorting)
    if len(filenames) == 0:
        print('No files given')
        return

    files = [open(x) for x in filenames]

    data = parse_files(files)
    offsets = [0]*len(files)

    if ordered(data, offsets):
        print('Everything is ordered!')
    else:
        print('Not ordered.')

    print('Offsets: {}'.format([x+1 for x in offsets]))

    for f in files:
        f.close()


def ordered(data, offsets):
    while(True):
        for p, offset in enumerate(offsets):
            txn = data[offset][p]

            for partition in txn['nodes']:
                if data[offsets[partition]][partition]['id'] != txn['id']:
                    break
            else:
                print('Execute', txn['id'], 'successfully!')
                for partition in txn['nodes']:
                    offsets[partition] += 1
                    if offsets[partition] == len(data):
                        return True
                break
        else:
            return False


def parse_files(files):
    data = []
    while True:
        data_line = []
        for f in files:
            line = f.readline().strip()
            if not line:
                return data
            data_line.append(parse_line(line))

        data.append(data_line)
    return data


def parse_line(line):
    result = {}
    elems = line.split(':')
    result['id'] = int(elems[0])
    result['nodes'] = [int(x) for x in elems[1].split(',')]
    return result


def natural_sorting(s):
    return [int(c) if c.isdigit() else c for c in re.split('([0-9]+)', s)]

if __name__ == '__main__':
    main()
