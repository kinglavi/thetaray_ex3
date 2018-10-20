from pyspark.sql.functions import concat, lit, regexp_replace, concat_ws


def add_shipment_path_column(shipment_df):
    # Calculate path to destination
    shipment_df = shipment_df.withColumn("full_path", concat_ws(
        ",",
        shipment_df.dest1,
        shipment_df.dest2,
        shipment_df.dest3,
        shipment_df.dest4,
        shipment_df.dest5
    ))

    return shipment_df
