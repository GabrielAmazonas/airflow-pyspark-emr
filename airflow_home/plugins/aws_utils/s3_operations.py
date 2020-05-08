import awswrangler as wr
import configparser



def check_output_quality(output_folder):
    # Config parser
    config = configparser.ConfigParser()
    config.read('airflow_home/dl.cfg')

    dl_output_data = config['DATALAKE']['OUTPUT_DATA']

    # We read a few records of the written parquet files to check the presence of some columns
    parquet_files = wr.s3.list_objects('s3://' + dl_output_data + '/' + output_folder)

    not_temporary_parquet_files = filter(lambda x: 'temporary' not in x, parquet_files)

    # Check if any records were inserted to the s3 table
    if len(not_temporary_parquet_files) == 0:
        raise ValueError("No rows persisted for the given table")

    res = [x for x in not_temporary_parquet_files]
    df = wr.s3.read_parquet(path=res[0],
                            chunked=10, dataset=True, use_threads=True)
    for row in df:
        if 'song_id' not in row.columns:
            raise ValueError("Failed to detect song_id column")

        if 'title' not in row.columns:
            raise ValueError("Failed to detect title column")


