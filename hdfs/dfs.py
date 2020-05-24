import logging
import subprocess

HDFS_DFS = 'hdfs dfs'


def ls(path=None, options=[]):
    commands = [HDFS_DFS, '-ls']

    if options:
        commands.extend(options)

    if path:
        commands.append(path)

    commands = ' '.join(commands)
    logging.info('commands: %s', commands)

    status, output = subprocess.getstatusoutput(commands)

    print('status: %s, output: %s' % (status, output))


if __name__ == '__main__':
    ls()
