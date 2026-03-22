import os
import dask_geopandas as dg
import geopandas as gpd
from botocore.exceptions import ClientError
import boto3


os.environ['PROJ_LIB'] = '/opt/conda/envs/vector_tutorial/share/proj'

def get_gdam_json(country_code, admin_level):
    """
    This function download geographic administrative boundaries from the GDAM website. User needs to check the website to make sure the requested admin_level is available for download. 

    
    Inputs:
    --------
    country_code: string
        Target country's name in ISO Alpha-3 format
    adming_level: int
        Administrative level for the target boundaries

    Returns:
    --------
    boundaries: Geopandas GeoDataFrame
        Geographic administrative boundaries of the country_code in Geopandas GeoDataFrame
    """

    url = f"https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_{country_code}_{admin_level}.json.zip"
    boundaries = gpd.read_file(url)
    boundaries = boundaries.rename(columns={f"NAME_{admin_level}": "Parish"})
    
    return boundaries
                  
def get_google_microsoft_bldgs(country_code, local_path, blocksize="256M"):
    """
    This function download geoparquet files from the Google Microsoft Building Footprint 
    - Combined by VIDA dataset hosted on Source Cooperative 
    (https://source.coop/repositories/vida/google-microsoft-open-buildings/description). 
    
    Inputs:
    --------
    country_code: string
        Target country's name in ISO Alpha-3 format
    s3_client: boto3 resource client
        A boto3 resource client configured with AWS credentials and the endpoint_url for Source Cooperative
    local_path: string
        Path to a local directory for downloading the geoparquet file(s)
    blocksize: string
        Blocksize used to load the geoparquet with Dask DataFrame. Default is 256M.

    Returns:
    --------
    bldg_ddf: Dask-Geopandas GeoDataFrame
        Buildings footprint dataset in a Dask-Geopandas GeoDataFrame with lazy loading. 
        
    """

    from botocore import UNSIGNED
    from botocore.client import Config  

    s3_client = boto3.client('s3',
                            endpoint_url='https://data.source.coop',
                            config=Config(signature_version=UNSIGNED)
                            )

    bucket_name = 'vida'
    prefix = f"google-microsoft-open-buildings/geoparquet/by_country/country_iso="
    country_prefix = f"{prefix}{country_code}/{country_code}.parquet"
    file_name = f"{country_code}.parquet"

    if not os.path.exists(local_path):
        os.mkdir(local_path)
            
    if not os.path.exists(f"{local_path}/{file_name}"):
        print(f"File not found locally. Downloading from s3...")
        
        try:
            s3_client.download_file(Bucket = bucket_name,
                                    Key = country_prefix,
                                    Filename = f"{local_path}/{file_name}"
                                    )
            print("Download complete.")
        except ClientError as error:
            if error.response["Error"]["Code"] == "404":
                print("The specified key does not exist in the bucket.")
            else:
                print(f"An error occurred: {error}")
        except Exception as error:
            print(f"An unexpected error occurred: {error}")
    else:
        print("File already exists locally. No download needed.")

    bldg_ddf = dg.read_parquet(f"{local_path}/{file_name}", gather_spatial_partitions=False, blocksize = blocksize)

    return bldg_ddf