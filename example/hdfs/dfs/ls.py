import sys
import logging

sys.path.append('.')

try:
    from hdfs import dfs
except ImportError:
    raise

logging.basicConfig(
    level=logging.INFO,
    format='%s\t%s\t%s:%s:%s\t%s' % (
        '%(levelname)s',
        '%(asctime)s',
        '%(filename)s',
        '%(funcName)s',
        '%(lineno)d',
        '%(message)s'
    )
)


if __name__ == '__main__':
    args = sys.argv

    path = None
    options = None

    if len(args) > 1:
        path = args[1]

    if len(args) > 2:
        options = args[2:]

    dfs.ls(path, options)
