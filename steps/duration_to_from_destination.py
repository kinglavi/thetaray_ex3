from pyspark import Row

DESTINATION_COLUMNS = ['dest1', 'dest2', 'dest3', 'dest4', 'dest5']


def calculate_duration_to_dest(rdd, destination):
    dict_row = rdd.asDict()
    total_duration = 0
    counter = 0

    for index in range(1, len(DESTINATION_COLUMNS) + 1):
        if dict_row['dest' + str(index)] == destination:
            total_duration = total_duration + dict_row['duretion' + str(index)]
            counter += 1

    dict_row['avg_duration_to_' + destination.lower()] = \
        total_duration / counter if counter != 0 else 0

    return Row(**dict_row)


def calculate_duration_from_dest(rdd, destination):
    dict_row = rdd.asDict()
    total_duration = 0
    counter = 0

    for index in range(1, len(DESTINATION_COLUMNS)):
        if dict_row['dest' + str(index)] == destination:
            total_duration = total_duration + dict_row['duretion' + str(index + 1)]
            counter += 1

    avg = total_duration / counter if counter != 0 else 0

    dict_row['avg_duration_from_' + destination.lower()] = avg

    return Row(**dict_row)


def calc_duration_to_from_destination(shipment_df, destination):
    shipment_df = shipment_df.rdd.map(
        lambda rdd: calculate_duration_to_dest(rdd, destination)).toDF()

    shipment_df = shipment_df.rdd.map(
        lambda rdd: calculate_duration_from_dest(rdd, destination)).toDF()

    return shipment_df
