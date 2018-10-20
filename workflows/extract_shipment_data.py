from steps.duration_to_from_destination import calc_duration_to_from_destination
from steps.find_shipment_path import add_shipment_path_column
from steps.initialize_data import create_init_df
from steps.total_duration import add_total_duration_column


def extract_shipment_data():
    shipment_df = create_init_df()

    shipment_df = add_shipment_path_column(shipment_df)

    shipment_df = add_total_duration_column(shipment_df)

    shipment_df = calc_duration_to_from_destination(shipment_df, "ISRAEL")

    result = shipment_df.select(
        "index", "name", "tx_date", "balance",
        "full_path", "total_duration", "avg_duration_to_israel", "avg_duration_from_israel"
    )

    result.show(truncate=False)
