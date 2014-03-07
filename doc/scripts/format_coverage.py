#!/usr/bin/python
import os
import argparse


def __format(fname):

    # Check coverage file exists.
    if not os.path.isfile(fname):
        msg = "The coverage report '%s' does not exist." % fname
        raise IOError(msg)

    # Read coverate report into list.
    with open(fname) as f:
        content = f.readlines()

    newcontent = list()
    heading = None
    class_name = None
    method_name = None
    for i in range(len(content)):

        if not content[i].strip():
            continue

        # NOTE: the coverage report does not pick up on undocumented
        #       methods. It looks like the only things it parses are objects?
        #       Untested assumptions here...
        if content[i].strip() == 'Classes:':
            continue

        if content[i].strip() == '-' * len(content[i].strip()):
            if heading:
                newcontent.append('\n')
            heading = content[i-1].strip()
            newcontent.append('.. warning::\n\n')
            newcontent.append('    :ref:`%s` has the following undocumented items.\n' % heading)

        if content[i].startswith(' * '):
            newcontent.append('\n')
            class_name = content[i].replace(' * ', '')
            class_name = class_name.replace(' -- missing methods:', '')
            class_name = class_name.strip()

        if content[i].startswith('   - '):
            method_name = content[i].strip()
            method_name = method_name.replace('- ', '')
            line = '%s- :py:class:`%s.%s`.%s()\n'
            line = line % (' ' * 8, heading, class_name, method_name)
            newcontent.append(line)

    with open(fname, 'w') as f:
        for line in newcontent:
            f.write(line)


if __name__ == '__main__':

    man = """Format coverage produced by 'sphinx-build -b coverage'.

             The coverage report produced by 'sphinx-build -b coverage'
             produces high-level headings which integrate poorly with the pyITS
             sphinx documentation. This script re-formats the undocumented
             items into warning directives.

          """

    parser = argparse.ArgumentParser(description=man)
    parser.add_argument('file', help="Location of coverage report.")
    args = parser.parse_args()

    __format(args.file)
