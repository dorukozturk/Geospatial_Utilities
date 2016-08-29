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


def convert_to_vrt(subdatasets, data_dir, bands):
    """
    Loops through the subdatasets and creates vrt files

    :param subdatasets: Subdataset of every HDF file
    :param data_dir: Result of create_output_directory method
    :return: None
    """
    data_list = []

    # 'bands' passed in from user refer to bands indexed from 1
    # make sure we decrement each band passed in so they we access the
    # 0 indexed band value inside the subdatasets list.
    for band in [b-1 for b in bands]:
        output_name = os.path.join(
            data_dir,
            "Band{}_{}.vrt".format(
                str(band + 1).zfill(2),
                subdatasets[band][0].split(":")[-1]))
        # Create the virtual raster

        gdal.BuildVRT(output_name, subdatasets[band][0])

        # Check if scale and offset exists
        scale = get_metadata_item(subdatasets[band][0], 'scale')

        modify_vrt(output_name, scale)

        data_list.append(output_name)

    return data_list

def clear_temp_files(data_dir, vrt_output):
    """ Removes the temporary files """

    os.remove(vrt_output)
    shutil.rmtree(data_dir)


def hdf2tif(hdf, tiff_path, bands=None, clobber=False,
            reproject=True, warpMemoryLimit=4096):
    """
    Converts hdf files to tiff files

    :param hdf: HDF file to be processed
    :param reproject: Will be reprojected by default
    :return: None
    """

    dataset = gdal.Open(hdf, gdal.GA_ReadOnly)
    subdatasets = dataset.GetSubDatasets()

    # Use bands passed in,  or list of all bands (indexed from 1)
    bands = bands if bands is not None else range(1, len(subdatasets) + 1)

    data_dir = create_output_directory(hdf)

    vrt_list = convert_to_vrt(subdatasets, data_dir, bands)
    vrt_options = gdal.BuildVRTOptions(separate=True)
    vrt_output = hdf.replace('.hdf', '.vrt')

    gdal.BuildVRT(vrt_output, vrt_list, options=vrt_options)
    if reproject:
        proj = "+proj=sinu +R=6371007.181 +nadgrids=@null +wktext"
        warp_options = gdal.WarpOptions(srcSRS=proj, dstSRS="EPSG:4326",
                                        warpMemoryLimit=warpMemoryLimit,
                                        multithread=True)
    else:
        warp_options = ""

    if not clobber and os.path.exists(tiff_path):
        raise RuntimeError("{} already exists, use '--clober' to overwrite"
                           % tiff_path)

    gdal.Warp(tiff_path,
              vrt_output, options=warp_options)

    metadata = []

    # Add the metadata
    for index, subd in enumerate(subdatasets):
        # Generate band names
        band_name = "{}:{}".format(str(index + 1).zfill(2),
                                   subd[0].split(":")[4])
        metadata.append(band_name)

    # Inject the metadata to the tiff
    gdal.Open(tiff_path).SetMetadata(str(metadata))

    clear_temp_files(data_dir, vrt_output)

    return tiff_path


class IntCSVParamType(click.ParamType):
    name = 'csv'

    def convert(self, value, param, ctx):
        try:
            if value is not None:
                return [int(b) for b in value.split(",")]
        except ValueError:
            self.fail('%s is not a valid comma seperated list of integers' % value, param, ctx)


@click.command()
@click.argument('hdf_file', type=click.Path(resolve_path=True))
@click.option('-o', '--output', default=None, type=click.Path(writable=True),
              help="Output file/directory")
@click.option('-b', '--bands', default=None, type=IntCSVParamType(),
              help="Only include specified bands (formated as csv)")
@click.option('-w', '--warpMemoryLimit', default=4096,
              help="Memory limit for Warp operation")
@click.option('--clobber/--no-clobber', default=False, help="Overwrite the created tiff")
@click.option('--reproject/--no-reproject', default=True, help="Reproject the tiff")
def main(hdf_file, output, bands, warpmemorylimit, clobber, reproject):
    """ Main function which orchestrates the conversion """
    import pudb; pu.db

    if output is None:
        output, _ = os.path.splitext(hdf_file)
        output += ".tiff"

    hdf2tif(hdf_file, output,
            bands=bands,
            warpMemoryLimit=warpmemorylimit,
            clobber=clobber,
            reproject=reproject)

if __name__ == "__main__":
    main()
