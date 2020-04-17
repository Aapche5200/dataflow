from __future__ import unicode_literals
from system_cmd import system_cmd_result


def test_one():
    system_cmd_result('.', 'ls -a')



def test_false():
    res = system_cmd_result('.', 'cp not-existing done')
    print(res)


def test_false2():
    res = system_cmd_result('.', 'cat UTF-8-test.txt',
                            display_stderr=True, display_stdout=True)
    print(res)
