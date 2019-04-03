"""
COMP9321 Assignment One Code Template 2019T1
Name: Amritesh Singh
Student ID: z5211987
"""

import pandas as pd
import csv
import json
from collections import defaultdict
import datetime


def title_style(name):
    ts = name.lower().split()

    for i in range(len(ts)):
        if ts[i].startswith('(l\'') or ts[i].startswith('(d\''):
            ts[i] = ts[i][:3] + ts[i][3:].capitalize()
        elif ts[i].startswith('l\'') or ts[i].startswith('d\''):
            if i == 0:
                ts[i] = ts[i][:2].capitalize() + ts[i][2:].capitalize()
            else:
                ts[i] = ts[i][:2] + ts[i][2:].capitalize()
        elif ts[i] not in ('la', 'de', '(la', '(de'):
            if ts[i].startswith('('):
                ts[i] = ts[i][0] + ts[i][1:].capitalize()
            elif len(ts[i].split('-')) > 1:
                ts[i] = '-'.join(j.capitalize() for j in ts[i].split('-'))
            elif len(ts[i].split('\'')) > 1:
                ts[i] = '\''.join(j.capitalize() for j in ts[i].split('\''))
            else:
                ts[i] = ts[i].capitalize()
        elif i == 0:
            ts[i] = ts[i].capitalize()

    return ts


def q1():
    data = pd.read_csv('accidents_2017.csv', float_precision='high')

    print(" ".join(f'\"{c}\"' if len(c.split()) > 1 else c for c in data.columns))

    for row in data.head(10).itertuples():
        row_contents = []

        for i in range(1, len(row)):
            if type(row[i]) is str:
                if i == 1:
                    row_contents.append(row[i].strip())
                else:
                    x = title_style(row[i])

                    if len(x) > 1:
                        row_contents.append(f'\"{" ".join(x)}\"')
                    else:
                        row_contents.append(" ".join(x))
            else:
                row_contents.append(str(row[i]))

        print(" ".join(row_contents))


def q2():
    data = pd.read_csv('accidents_2017.csv', float_precision='high')
    data.drop_duplicates(inplace=True)

    csv_header = [c for c in data.columns]
    csv_contents = []

    for row in data.itertuples():
        row_contents = []
        flag = True

        for i in range(1, len(row)):
            if type(row[i]) is str:
                if i == 1:
                    row_contents.append(row[i].strip())
                else:
                    x = title_style(row[i])

                    if " ".join(x) == "Unknown":
                        flag = False
                        break

                    row_contents.append(" ".join(x))
            else:
                row_contents.append(row[i])

        if flag:
            csv_contents.append(row_contents)

    data = pd.DataFrame(csv_contents, columns=csv_header)
    data.to_csv('result_q2.csv', sep=',', index=False, quoting=csv.QUOTE_NONNUMERIC)


def q3():
    data = pd.read_csv('accidents_2017.csv', float_precision='high')
    data.drop_duplicates(inplace=True)

    acc_dist = defaultdict(int)

    for row in data.itertuples():
        x = title_style(row._2)

        if len(x) > 1:
            x = f'\"{" ".join(x)}\"'
        else:
            x = " ".join(x)

        if x == "Unknown":
            continue

        if x not in acc_dist:
            acc_dist[x] = 1
        else:
            acc_dist[x] += 1

    print("\"District Name\" \"Total numbers of accidents\"")

    for i in sorted(acc_dist.items(), key=lambda k: k[1], reverse=True):
        print(f'{i[0]} {i[1]}')


def q4():
    # Part 1
    data1 = pd.read_csv('air_stations_Nov2017.csv', float_precision='high', na_filter=False)
    data1.drop_duplicates(inplace=True)

    stat_dst = []

    for row in data1.itertuples():
        stat_dst.append({'Station': row.Station, 'District Name': row._5})

    print(json.dumps(stat_dst))
    print()

    # Part 2
    data2 = pd.read_csv('air_quality_Nov2017.csv', float_precision='high', na_filter=False)
    data2.drop_duplicates(inplace=True)

    ctr = 0

    print(" ".join(f'\"{c}\"' if len(c.split()) > 1 else c for c in data2.columns))

    for row in data2.itertuples():
        if ctr == 10:
            break

        if row._2 not in ('Good', '--'):
            ctr += 1
            row_contents = []

            for i in range(1, len(row)):
                if type(row[i]) is str and row[i] != 'NA':
                    x = title_style(row[i])

                    if len(x) > 1:
                        row_contents.append(f'\"{" ".join(x)}\"')
                    else:
                        row_contents.append(" ".join(x))
                else:
                    row_contents.append(str(row[i]))

            print(" ".join(row_contents))

    # Part 3
    data_merged = pd.merge(data1[['Station', 'District Name']], data2[['Station', 'Air Quality', 'Generated']], how='inner', on="Station")
    dt = []

    for row in data_merged.itertuples():
        if row._3 in ('Good', '--'):
            continue

        hour = int(row.Generated.split()[1].split(':')[0])
        day = int(row.Generated.split('/')[0])
        month = str(datetime.date(1900, int(row.Generated.split('/')[1]), 1).strftime('%B'))

        if (row._2, month, day, hour) not in dt:
            dt.append((row._2, month, day, hour))

    data3 = pd.read_csv('accidents_2017.csv', float_precision='high')
    data3.drop_duplicates(inplace=True)

    csv_header = [c for c in data3.columns]
    csv_contents = []

    for row in data3.itertuples():
        if (row._2, row.Month, row.Day, row.Hour) not in dt:
            continue

        row_contents = []
        flag = True

        for i in range(1, len(row)):
            if type(row[i]) is str:
                if i == 1:
                    row_contents.append(row[i].strip())
                else:
                    x = title_style(row[i])

                    if " ".join(x) == "Unknown":
                        flag = False
                        break

                    row_contents.append(" ".join(x))
            else:
                row_contents.append(row[i])

        if flag:
            csv_contents.append(row_contents)

    data3 = pd.DataFrame(csv_contents, columns=csv_header)
    data3.to_csv('result_q4.csv', sep=',', index=False, quoting=csv.QUOTE_NONNUMERIC)


def q5():
    pass


if __name__ == '__main__':
    q1()
    q2()
    q3()
    q4()
    # q5()
