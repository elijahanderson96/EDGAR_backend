def find_new_records(df_new, df_existing, compare_columns):
    """
    Memory-efficient version that doesn't create temporary columns.
    """
    # Ensure the compare_columns exist in both DataFrames
    for col in compare_columns:
        if col not in df_new.columns or col not in df_existing.columns:
            raise ValueError(f"Column '{col}' not found in both DataFrames")

    # Merge with indicator to find new records
    merged = df_new.merge(
        df_existing[compare_columns],
        on=compare_columns,
        how='left',
        indicator=True
    )

    # Return only records that exist only in df_new
    new_records = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

    return new_records
