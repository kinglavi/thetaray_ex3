def add_total_duration_column(shipment_df):
    return shipment_df.withColumn(
        "total_duration",
        shipment_df.duretion1 +
        shipment_df.duretion2 +
        shipment_df.duretion3 +
        shipment_df.duretion4 +
        shipment_df.duretion5)
