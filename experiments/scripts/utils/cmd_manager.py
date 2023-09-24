from fabric import Connection

env_cmd = 'source ~/.zshrc'


class CMDManager(object):
    def __init__(self, cluster_ips: list):
        self._cluster_ips = cluster_ips
        self._conn_list = [Connection(ip) for ip in self._cluster_ips]

    def execute_all(self, cmd):
        prom_list = []
        for c in self._conn_list:
            prom = c.run(f'{env_cmd} && {cmd}', asynchronous=True)
            prom_list.append(prom)
        for prom in prom_list:
            prom.join()

    def execute_on_nodes(self, node_ids, cmd):
        prom_list = []
        for id in node_ids:
            c = self._conn_list[id]
            prom = c.run(f'{env_cmd} && {cmd}', asynchronous=True)
            prom_list.append(prom)
        for prom in prom_list:
            try:
                prom.join()
            except:
                pass

    def execute_on_node(self, node_id, cmd, asynchronous=True):
        return self._conn_list[node_id].run(f'{env_cmd} && {cmd}', asynchronous=asynchronous)

    def get_file(self, node_id, remote, local):
        node_sftp = self._conn_list[node_id].sftp()
        node_sftp.get(remote, local)
