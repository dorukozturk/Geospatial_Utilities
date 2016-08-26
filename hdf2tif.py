import glob
import os
import shutil
from xml.etree.ElementTree import parse, SubElement

import click
import gdal


DIRECTORY = os.path.dirname(os.path.realpath(__file__))


def list_files(directory, extension):
    """
    Lists all the files in a given directory with a wildcard

    :param directory: Directory to be checked
    :param extension: File extension to be searched
    :return: List of files matching the extension
    """

    return glob.glob(os.path.join(directory, '*.{}'.format(extension)))


def create_output_directory(hdf):
    """
    Creates a unique output directory to store the intermediate vrt files

    :param hdf: HDF file to be processed
    :return: Folder
    """

    direc = os.path.splitext(hdf)[0]
    if not os.path.exists(direc):
        os.makedirs(direc)

    return direc


def get_metadata_item(subdataset, keyword):
    """
    Checks for keyword in metadata and returns if it exists

    :param subdataset: HDF subdataset
    :param keyword: Keyword to
    :return: Metadata item
    """

    dataset = gdal.Open(subdataset, gdal.GA_ReadOnly)

    metadata = dataset.GetMetadata_Dict()

    # Filter the metadata
    filtered_meta = {k: v for k, v in metadata.iteritems()
                     if keyword in k.lower()}

    # Hopefully there will be one element in the dictionary
    return filtered_meta[filtered_meta.keys()[0]]



def modify_vrt(vrt, scale):
    """
    Makes modifications to the vrt file to fix the values.

    :param vrt: VRT file to be processed
    :param scale: Scale value from get_metadata_item function
    :return: None
    """

    doc = parse(vrt)

    root = doc.getroot()

    # Fix the datatype if it is wrong
    raster_band = root.find('VRTRasterBand')
    raster_band.set('dataType', 'Float32')

    # Add the scale to the vrt file
    source = root.find('VRTRasterBand').find('ComplexSource')
    scale_ratio = SubElement(source, 'ScaleRatio')
    scale_ratio.text = scale

    # Write the scale input
    # vrt files are overwritten with the same name
    doc.write(vrt, xml_declaration=True)


def convert_to_vrt(subdatasets, data_dir):
    """
    Loops through the subdatasets and creates vrt files

    :param subdatasets: Subdataset of every HDF file
    :param data_dir: Result of create_output_directory method
    :return: None
    """

    # For every subdataset loop through the subdatasets
    # Enumerate to keep the bands in order

    for index, subd in enumerate(subdatasets):

        # Generate output name from the band names
        output_name = os.path.join(
            data_dir,
            "Band_{}_{}.vrt".format(str(index + 1).zfill(2),
                                    subd[0].split(":")[4]))

        # Create the virtual raster
        gdal.BuildVRT(output_name, subd[0])

        # Check if scale and offset exists
        scale = get_metadata_item(subd[0], 'scale')

        modify_vrt(output_name, scale)


def clear_temp_files(data_dir, vrt_output):
    """ Removes the temporary files """

    os.remove(vrt_output)
    shutil.rmtree(data_dir)


def hdf2tif(hdf, reproject=True):
    """
    Converts hdf files to tiff files

    :param hdf: HDF file to be processed
    :param reproject: Will be reprojected by default
    :return: None
    """

    dataset = gdal.Open(hdf, gdal.GA_ReadOnly)
    subdatasets = dataset.GetSubDatasets()
    data_dir = create_output_directory(hdf)
    convert_to_vrt(subdatasets, data_dir)
    vrt_options = gdal.BuildVRTOptions(separate=True)
    vrt_list = list_files(data_dir, 'vrt')
    vrt_output = hdf.replace('.hdf', '.vrt')
    gdal.BuildVRT(vrt_output, sorted(vrt_list), options=vrt_options)
    if reproject:
        proj = "+proj=sinu +R=6371007.181 +nadgrids=@null +wktext"
        warp_options = gdal.WarpOptions(srcSRS=proj, dstSRS="EPSG:3857")
    else:
        warp_options = ""
    output_tiff = vrt_output.replace(".vrt", "_reprojected.tif")

    if not os.path.exists(output_tiff):
        gdal.Warp(output_tiff,
              vrt_output, options=warp_options)

    clear_temp_files(data_dir, vrt_output)

    return output_tiff

@click.command()
@click.option('--hdf_file', help="Input hdf file")
def main(hdf_file):
    """ Main function which orchestrates the conversion """

    hdf2tif(hdf_file)

if __name__ == "__main__":
    main()
