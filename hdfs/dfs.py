import logging
import subprocess
import re

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
    if status:
        raise Exception(output)

    lines = output.split('\n')
    lines = lines[1:]

    infos = []

    regex = (
        '^'
        '([\\S]{10})'
        '[\\s]+'
        '([\\S]+)'
        '[\\s]+'
        '([\\S]+)'
        '[\\s]+'
        '([\\S]+)'
        '[\\s]+'
        '([\\S]+)'
        '[\\s]+'
        '([\\d]{4}-[\\d]{2}-[\\d]{2} [\\d]{2}:[\\d]{2})'
        '[\\s]+'
        '([\\S]+)'
        '$'
    )

    pattern = re.compile(regex.strip())

    for line in lines:
        match = pattern.match(line)
        if not match:
            continue

        permissions = match.group(1)
        replicas = match.group(2)
        userId = match.group(3)
        groupId = match.group(4)
        size = match.group(5)
        modification = match.group(6)
        path = match.group(7)

        info = {}

        info['type'] = 'directory' if permissions.startswith('d') else 'file'
        info['permissions'] = permissions[1:]
        info['replicas'] = replicas
        info['userId'] = userId
        info['groupId'] = groupId
        info['size'] = size
        info['modification'] = modification
        info['path'] = path

        infos.append(info)

    return infos


if __name__ == '__main__':
    ls()
