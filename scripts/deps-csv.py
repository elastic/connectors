#!/usr/bin/python3

import csv
import sys

# input csv column indices
NAME = 0
VERSION = 1
LICENSE = 2
URL = 3


def main(dependencies_csv):
    """
    The input is what we get from `pip-licenses --format=csv --with-urls`
    See: https://pypi.org/project/pip-licenses/#csv
    Unfortunately, our DRA requires a few more columns that `pip-licenses` does not understand.
    This function reorders each row.
    :param dependencies_csv:
    :return:
    """
    rows = []

    # read the csv rows into memory
    with open(dependencies_csv) as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            rows.append(row)

    # overwrite the original file
    with open(dependencies_csv, "w") as csv_file:
        writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)

        # The expected column order (this row is the CSV header)
        writer.writerow(["name", "url", "version", "revision", "license", "sourceURL"])

        # reorder each row using the expected column order. (leaves 'revision' and 'sourceURL' empty)
        for row in rows[1:]:  # skip the header row
            writer.writerow([row[NAME], row[URL], row[VERSION], "", row[LICENSE], ""])


if __name__ == "__main__":
    depenencies_csv = sys.argv[1]
    print(f"post-processing {depenencies_csv}")  # noqa
    main(depenencies_csv)
    print(f"wrote output to {depenencies_csv}")  # noqa
