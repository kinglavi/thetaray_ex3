from datetime import timedelta, datetime
from random import randint

from general_utils.spark_utils import get_spark_context

COUNT = 20
NAME_LIST = ["Arika", "Bob", "Corry", "David", "Trump"]
DESTINATION_LIST = ["USA", "SPAIN", "ISRAEL", "SWEDEN", None]
START_DATE = datetime(2018, 1, 1, 10, 12, 14)
DOUBLE_LIST = [1.1, 2.2, 3.3, 4.4]


def create_init_df():
    spark = get_spark_context()[0]

    l = []
    for i in range(0, COUNT):
        l.append((
            i,
            NAME_LIST[i % len(NAME_LIST)],
            DESTINATION_LIST[randint(1, len(DESTINATION_LIST) - 2)],
            randint(1, 50),
            DESTINATION_LIST[randint(1, len(DESTINATION_LIST) - 1)],
            randint(1, 50),
            DESTINATION_LIST[randint(1, len(DESTINATION_LIST) - 1)],
            randint(1, 50),
            DESTINATION_LIST[randint(1, len(DESTINATION_LIST) - 1)],
            randint(1, 50),
            DESTINATION_LIST[randint(1, len(DESTINATION_LIST) - 1)],
            randint(1, 50),
            START_DATE + timedelta(days=5 * i),
            DOUBLE_LIST[i % len(DOUBLE_LIST)]))

    df = spark.createDataFrame(l, ['index', 'name', 'dest1', 'duretion1', 'dest2', 'duretion2', 'dest3', 'duretion3',
                                   'dest4', 'duretion4', 'dest5', 'duretion5', 'tx_date', 'balance'])

    df = df.persist()
    print "Create data frame - count:", df.count()

    return df
