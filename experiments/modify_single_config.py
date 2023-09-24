import os
import sys
import json


def parse_args(argv: list):
    print(argv)
    modify_dict = {}
    for kv in argv:
        key = kv.split('=')[0]
        val = json.loads(kv.split('=')[1])
        modify_dict[key] = val
    return modify_dict


def get_config_fname_list(config_name):
    server_fname = "server_conf_{}.json".format(config_name)
    client_fname = "client_conf_{}.json".format(config_name)
    return [server_fname, client_fname]


def modify_config_files(cfname_list, modify_dict: dict):
    for c in cfname_list:
        fname = '../configs/{}'.format(c)
        with open(fname, 'r') as f:
            content = json.load(f)
        for k in modify_dict:
            content[k] = modify_dict[k]
        with open(fname, 'w') as f:
            json.dump(content, f, indent=True)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: {} <config_name> <key=val>".format(sys.argv[0]))
        exit(0)

    cfname_list = get_config_fname_list(sys.argv[1])
    modify_dict = parse_args(sys.argv[2:])
    modify_config_files(cfname_list, modify_dict)
