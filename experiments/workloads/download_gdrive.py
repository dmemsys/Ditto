import gdown
import sys

url = 'https://drive.google.com/uc?id={}&export=download'.format(sys.argv[1])
output = '{}'.format(sys.argv[2])
gdown.download(url, output, quiet=False)