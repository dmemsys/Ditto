import os
import sys
import json

config_fname_list = os.listdir('./configs')


def parse_args(argv: list):
    modify_dict = {}
    for kv in argv:
        key = kv.split('=')[0]
        val = json.loads(kv.split('=')[1])
        modify_dict[key] = val
    return modify_dict


def modify_config_files(modify_dict: dict):
    cfname_list = os.listdir('./configs')
    for c in cfname_list:
        fname = './configs/{}'.format(c)
        with open(fname, 'r') as f:
            content = json.load(f)
        for k in modify_dict:
            content[k] = modify_dict[k]
        with open(fname, 'w') as f:
            json.dump(content, f, indent=True)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: {} key=val".format(sys.argv[0]))
        exit(0)

    modify_dict = parse_args(sys.argv[1:])
    modify_config_files(modify_dict)
