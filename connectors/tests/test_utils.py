from connectors.utils import next_run


def test_next_run():

    assert next_run("1 * * * * *") < 70.0
    assert next_run("* * * * * *") == 0
    assert next_run("0/5 14,18,3-39,52 * ? JAN,MAR,SEP MON-FRI 2002-2010") > 0
