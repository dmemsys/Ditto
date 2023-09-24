lineStyleList = [(0, (5, 1)), (0, (3, 1, 1, 1, 1, 1)),
                 (0, (3, 1, 1, 1)), (0, ()), (0, (1, 1))]
lineColorDict = {
    'precise-lru': '#012d5a',
    'shard-lru': '#012d5a',
    'cliquemap-precise-lru': '#0066cc',
    'cliquemap-precise-lfu': '#70BFFF',
    'cliquemap-shard-lru': '#0066cc',
    'cliquemap-shard-lfu': '#70BFFF',
    'redis': '#0066cc',
    'sample-lru': '#ff9900',
    'sample-lfu': '#ffc000',
    'sample-adaptive': '#d63026',
    'sample-adaptive-cpu': '#d63026',
    'sample-adaptive-mem': '#ff8000',
    'kvs': '#d63026',
    'non': '#d63026',
}
lineStyleDict = {
    'precise-lru': (0, ()),
    'shard-lru': (0, ()),
    'cliquemap-precise-lru': (0, ()),
    'cliquemap-precise-lfu': (0, (5, 1)),
    'cliquemap-shard-lru': (0, ()),
    'cliquemap-shard-lfu': (0, (5, 1)),
    'redis': (0, ()),
    'sample-lru': (0, ()),
    'sample-lfu': (0, (5, 1)),
    'sample-adaptive': (0, ()),
    'sample-adaptive-cpu': (0, ()),
    'sample-adaptive-mem': (0, (3, 1, 1, 1, 1, 1)),
    'kvs': (0, ()),
    'non': (0, ())
}
barColorDict = {
    'precise-lru': '#e1ebf3',
    'shard-lru': '#e1ebf3',
    'cliquemap-precise-lfu': '#0066cc',
    'cliquemap-precise-lru': '#b3d9ff',
    'cliquemap-shard-lru': '#0066cc',
    'cliquemap-shard-lfu': '#b3d9ff',
    'sample-lru': '#ffc000',
    'sample-lfu': '#faeadc',
    'sample-adaptive': '#ffa09e',
    'kvs': '#ffa09e',
    'non': '#ffa09e'
}
barHatchDict = {
    'precise-lru': '--',
    'shard-lru': '--',
    'cliquemap-precise-lfu': 'xxx',
    'cliquemap-precise-lru': '//',
    'cliquemap-shard-lru': 'xxx',
    'cliquemap-shard-lfu': '//',
    'sample-lru': '--',
    'sample-lfu': '||',
    'sample-adaptive': '\\\\',
    'kvs': '\\\\',
    'non': '\\\\'
}
lineMarkerDict = {
    'precise-lru': 'd',
    'shard-lru': 'd',
    'cliquemap-precise-lru': 'o',
    'cliquemap-precise-lfu': 'x',
    'cliquemap-shard-lru': 'o',
    'cliquemap-shard-lfu': 'x',
    'redis': 'd',
    'sample-lru': 'd',
    'sample-lfu': '+',
    'sample-adaptive': '^',
    'kvs': '^',
    'non': '^'
}
methodLabelDict = {
    'precise-lru': 'P-LRU',
    'shard-lru': 'Shard-LRU',
    'cliquemap-precise-lru': 'CM-LRU',
    'cliquemap-precise-lfu': 'CM-LFU',
    'cliquemap-shard-lru': 'CM-LRU',
    'cliquemap-shard-lfu': 'CM-LFU',
    'sample-lru': 'Ditto-LRU',
    'sample-lfu': 'Ditto-LFU',
    'sample-adaptive': 'Ditto',
    'sample-adaptive-cpu': 'Ditto Scale CPU',
    'sample-adaptive-mem': 'Ditto Scale Memory',
    'redis': 'Redis',
    'kvs': 'KVS',
    'non': 'KVS'
}
zorderDict = {
    'precise-lru': 10,
    'shard-lru': 10,
    'cliquemap-precise-lru': 11,
    'cliquemap-precise-lfu': 12,
    'cliquemap-shard-lru': 11,
    'cliquemap-shard-lfu': 12,
    'sample-lru': 13,
    'sample-lfu': 14,
    'sample-adaptive': 15,
    'kvs': 16,
    'non': 16
}
