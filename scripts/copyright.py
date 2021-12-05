# -*- coding: utf-8 -*-
# Copyright (c) 2020-present, UMD Database Group.
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import argparse
import io, re
import sys, os
import subprocess
import platform

COPYRIGHT = '''
Copyright (c) 2020-present, UMD Database Group.

This program is free software: you can use, redistribute, and/or modify
it under the terms of the GNU Affero General Public License, version 3
or later ("AGPL"), as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
'''

LANG_COMMENT_MARK = None

NEW_LINE_MARK = None

COPYRIGHT_HEADER = None

NEW_LINE_MARK = '\n'
COPYRIGHT_HEADER = COPYRIGHT.split(NEW_LINE_MARK)[1]
p = re.search('(\d{4})', COPYRIGHT_HEADER).group(0)
process = subprocess.Popen(["date", "+%Y"], stdout=subprocess.PIPE)
date, err = process.communicate()
date = date.decode("utf-8").rstrip("\n")
COPYRIGHT_HEADER = COPYRIGHT_HEADER.replace(p, date)


def generate_copyright(template, lang='go'):
    if lang in ['Python', 'shell', 'yaml']:
        LANG_COMMENT_MARK = '#'
    else:
        LANG_COMMENT_MARK = "//"

    lines = template.split(NEW_LINE_MARK)
    BLANK = " "
    ans = LANG_COMMENT_MARK + BLANK + COPYRIGHT_HEADER + NEW_LINE_MARK
    for lino, line in enumerate(lines):
        if lino == 0 or lino == 1 or lino == len(lines) - 1: continue
        if len(line) == 0:
            BLANK = ""
        else:
            BLANK = " "
        ans += LANG_COMMENT_MARK + BLANK + line + NEW_LINE_MARK

    return ans + "\n"


def lang_type(filename):
    if filename.endswith(".py"):
        return "Python"
    elif filename.endswith(".go"):
        return "go"
    elif filename.endswith(".proto"):
        return "go"
    elif filename.endswith(".sh"):
        return "shell"
    elif filename.endswith(".java"):
        return "Java"
    elif filename.endswith(".yaml"):
        return "yaml"
    elif filename.endswith(".rs"):
        return "rust"
    else:
        print("Unsupported filetype %s", filename)
        exit(0)


PYTHON_ENCODE = re.compile("^[ \t\v]*#.*?coding[:=][ \t]*([-_.a-zA-Z0-9]+)")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Checker for copyright declaration.')
    parser.add_argument('filenames', nargs='*', help='Filenames to check')
    args = parser.parse_args(argv)

    retv = 0
    for filename in args.filenames:
        fd = io.open(filename, encoding="utf-8")
        first_line = fd.readline()
        second_line = fd.readline()
        third_line = fd.readline()
        # check for 3 head lines
        if "COPYRIGHT " in first_line.upper(): continue
        if "COPYRIGHT " in second_line.upper(): continue
        if "COPYRIGHT " in third_line.upper(): continue
        skip_one = False
        skip_two = False
        if first_line.startswith("#!"):
            skip_one = True
        if PYTHON_ENCODE.match(second_line) != None:
            skip_two = True
        if PYTHON_ENCODE.match(first_line) != None:
            skip_one = True

        original_content_lines = io.open(filename,
                                         encoding="utf-8").read().split("\n")
        copyright_string = generate_copyright(COPYRIGHT, lang_type(filename))
        if skip_one:
            new_contents = "\n".join(
                [original_content_lines[0], copyright_string] +
                original_content_lines[1:])
        elif skip_two:
            new_contents = "\n".join([
                original_content_lines[0], original_content_lines[1],
                copyright_string
            ] + original_content_lines[2:])
        else:
            new_contents = generate_copyright(
                COPYRIGHT,
                lang_type(filename)) + "\n".join(original_content_lines)
        print('Auto Insert Copyright Header {}'.format(filename))
        with io.open(filename, 'w', encoding='utf8') as output_file:
            output_file.write(new_contents)

    return retv


if __name__ == '__main__':
    exit(main())
