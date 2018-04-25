#!/usr/bin/python3
import sys
import re
from functools import reduce

def main():
    action = sys.argv[1]
    filenames = sys.argv[2:]
    filenames.sort(key=natural_sorting)
    if len(filenames) == 0:
        print('No files given')
        return

    files = [open(x) for x in filenames]

    data = parse_files(files)

    if action == 'order':
        offsets = [0]*len(files)

        if easy_order(data):
            print('Everything is ordered with easy_order!')
        else:
            print('Not ordered.')
            return

        if ordered(data, offsets):
            print('Everything is ordered!')
        else:
            print('Not ordered.')

        print('Offsets: {}'.format([x+1 for x in offsets]))
        print('Total: {}'.format(len(data)))
    elif action == 'stat':
        stat_on_round(data)

    for f in files:
        f.close()


def stat_on_round(data):
    for partition_id in range(len(data[0])):
        stats = [0] * 10
        average = 0
        count = 0
        for i in range(len(data)):
            elem = data[i][partition_id]
            if elem is None:
                continue
            if elem['round'] == -1:
                continue
            while len(stats) <= elem['round']:
                stats.append(0)

            stats[elem['round']] += 1
            average += elem['round']
            count += 1

        stats = [x for x in zip(range(len(stats)), stats) if x[1] != 0]
        average = average/count
        print('Partition {}: {} {}'.format(partition_id, average, stats))


def easy_order(data):
    for partition_id in range(len(data[0])):
        previous = -1
        for i in range(len(data)):
            elem = data[i][partition_id]
            if not elem:
                continue
            if not previous <= elem['timestamp']:
                if len(elem['nodes']) == 1 and elem['timestamp'] == 0:
                    # We have an SPO
                    continue
                print('Order is broken at transaction {} with timestamp {} for parition {}'.format(elem['id'], elem['timestamp'], partition_id))
                return False
            previous = elem['timestamp']
    return True

def ordered(data, offsets):
    while(True):
        hasTxnExecuted = False
        for p, offset in enumerate(offsets):
            if len(data) <= offset:
                return True
            txn = data[offset][p]
            if txn is None:
                continue

            executable = True
            for partition in txn['nodes']:
                val = data[offsets[partition]][partition]

                if val is None or val['id'] != txn['id']:
                    executable = False
            if executable:
                hasTxnExecuted = True
                print('Execute', txn['id'], 'successfully!')
                for partition in txn['nodes']:
                    offsets[partition] += 1
                break
        if not hasTxnExecuted:
            for i in range(len(offsets)):
                if data[offsets[i]][i] is None:
                    return True
            return False
    return False


def parse_files(files):
    data = []
    while True:
        data_line = []
        for f in files:
            line = f.readline().strip()
            if not line:
                data_line.append(None)
            else:
                data_line.append(parse_line(line))

        if all(x is None for x in data_line):
            return data
        else:
            data.append(data_line)

    return data


def parse_line(line):
    result = {}
    elems = line.split(':')
    result['id'] = int(elems[0])
    result['nodes'] = [int(x) for x in elems[1].split(',')]
    result['round'] = int(elems[2])
    result['timestamp'] = int(elems[3])
    return result


def natural_sorting(s):
    return [int(c) if c.isdigit() else c for c in re.split('([0-9]+)', s)]

if __name__ == '__main__':
    main()
