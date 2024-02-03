if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    rows_removed = (data['passenger_count'].fillna(0).isin([0]) | data['trip_distance'].fillna(0).isin([0]))
    df = data.loc[~rows_removed,:]
    # rows_removed = data.loc[data['passenger_count'].fillna(0).isin([0]) | data['trip_distance'].fillna(0).isin([0]) ,:].shape[0]
    # df = data.loc[~(data['passenger_count'].fillna(0).isin([0]) | data['trip_distance'].fillna(0).isin([0])) ,:]
    print(f"Removed {rows_removed.sum()} rows where the passenger count is equal to 0 or the trip distance is equal to zero.")
    
    print("Following column names were converted to camel_case:")
    for c in df.columns:
        camel = c.lower().replace(' ','_')
        if c!=camel:
            print (f'{c} -> {camel}' )
    df.columns = df.columns.str.replace(" ","_").str.lower()

    df["lpep_pickup_date"] = df["lpep_pickup_datetime"].dt.date

    # print(sorted(list(df.vendorid.fillna(-1).unique())))

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert sorted(list(output.vendorid.fillna(-1).unique())) ==[1,2], 'vendorid value is not in the allowed list'
    assert output['passenger_count'].fillna(0).isin([0]).sum() ==0 , 'passenger_count is not greater than 0'
    assert output['trip_distance'].fillna(0).isin([0]).sum() ==0 , 'trip_distance is not greater than 0'
