import sys

def main():
    filenames = sorted(sys.argv[1:])
    if len(filenames) == 0:
        print('No files given')
        return

    files = [open(x) for x in filenames]

    data = parse_files(files)
    offsets = [0]*len(files)

    if ordered(data, offsets):
        print('Everything is ordered!')
    else:
        print('Not ordered. Line blocking: {}'.format([x+1 for x in offsets]))

    for f in files:
        f.close()


def ordered(data, offsets):
    while(True):
        for p, offset in enumerate(offsets):
            if offset == len(data):
                return True
            txn = data[offset][p]
            #  if len(txn['nodes']) == 1:
                #  offsets[i] += 1
                #  break

            executable = True
            for partition in txn['nodes']:
                if data[offsets[partition]][partition]['id'] != txn['id']:
                    executable = False
            if executable:
                for partition in txn['nodes']:
                    offsets[partition] += 1
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


if __name__ == '__main__':
    main()
