import multiprocessing
import errno
import glob
import os
import shutil
from xml.etree.ElementTree import parse, SubElement

import click
import gdal
from utils import IntCSVParamType, TemporaryDirectory

DIRECTORY = os.path.dirname(os.path.realpath(__file__))

NO_DATA = -9999


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

        # Get the fill value
        fill_value = get_metadata_item(subdatasets[band][0], 'fillvalue')

        # Pass some options
        vrt_options = gdal.BuildVRTOptions(srcNodata=fill_value, VRTNodata=NO_DATA)

        # Create the virtual raster
        gdal.BuildVRT(output_name, subdatasets[band][0], options=vrt_options)

        # Check if scale and offset exists
        scale = get_metadata_item(subdatasets[band][0], 'scale')

        modify_vrt(output_name, scale)

        data_list.append(output_name)

    return data_list

def hdf2tif(hdf, tiff_path, bands=None, clobber=False,
            reproject=True, warpMemoryLimit=4096):
    """
    Converts hdf files to tiff files

    :param hdf: HDF file to be processed
    :param reproject: Will be reprojected by default
    :return: None
    """

    basename, _ = os.path.splitext(os.path.basename(hdf))

    dataset = gdal.Open(hdf, gdal.GA_ReadOnly)
    subdatasets = dataset.GetSubDatasets()

    # Use bands passed in,  or list of all bands (indexed from 1)
    bands = bands if bands is not None else range(1, len(subdatasets) + 1)

    # data_dir = create_output_directory(hdf)
    with TemporaryDirectory() as data_dir:
        vrt_list = convert_to_vrt(subdatasets, data_dir, bands)
        vrt_options = gdal.BuildVRTOptions(separate=True, srcNodata=NO_DATA)
        vrt_output = os.path.join(data_dir, basename + ".vrt")

        gdal.BuildVRT(vrt_output, vrt_list, options=vrt_options)

        if reproject:
            proj = "+proj=sinu +R=6371007.181 +nadgrids=@null +wktext"
            warp_options = gdal.WarpOptions(srcSRS=proj, dstSRS="EPSG:4326",
                                            warpMemoryLimit=warpMemoryLimit,
                                            multithread=True)
        else:
            warp_options = ""

        if not clobber and os.path.exists(tiff_path):
            raise RuntimeError(
                "{} already exists, use '--clober' to overwrite".format(tiff_path))

        gdal.Warp(tiff_path,
                  vrt_output, options=warp_options)


        meta = dataset.GetMetadata()

        # Add the metadata
        for idx, band in enumerate(bands):
            # Generate band names
            key = "BAND_{}_NAME".format(idx + 1)
            meta[key] = str(subdatasets[band - 1][0].split(":")[4])

        dataset = gdal.Open(tiff_path, gdal.GA_Update)

        # Inject the metadata to the tiff

        meta['BANDS'] = str(meta)
        dataset.SetMetadata(meta)

        # Inject the band statistics so that
        # we do not have to enter them
        for band in range(dataset.RasterCount):
            srcband = dataset.GetRasterBand(band+1).ComputeStatistics(0)

        # Flush the dataset
        dataset = None

    return tiff_path


def serial_process(hdf_files, **kwargs):
    for hdf_file in hdf_files:
        process_file((hdf_file, kwargs))


def process_file((hdf_file, kwargs)):
    output_dir = kwargs.pop("output_dir", None)

    if output_dir is None:
        output_dir = os.path.dirname(hdf_file)
    try:
        os.makedirs(output_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    file_base, ext = os.path.splitext(os.path.basename(hdf_file))

    hdf2tif(hdf_file, os.path.join(output_dir, file_base + ".tiff"),
            **kwargs)

@click.command()
@click.argument('hdf_files', nargs=-1,
                type=click.Path(exists=True, resolve_path=True))
@click.option('-o', '--output', default=None,
              type=click.Path(file_okay=False, writable=True),
              help="Output file/directory")
@click.option('-b', '--bands', default=None, type=IntCSVParamType(),
              help="Only include specified bands (formated as csv)")
@click.option('-w', '--warpMemoryLimit', default=4096,
              help="Memory limit for Warp operation")
@click.option('-j', '--jobs', default=0, help="Number of Processes in pool")
@click.option('--clobber/--no-clobber', default=False, help="Overwrite the created tiff")
@click.option('--reproject/--no-reproject', default=True, help="Reproject the tiff")
def main(hdf_files, output, bands, warpmemorylimit, jobs, clobber, reproject):
    """ Main function which orchestrates the conversion """
    kwargs = dict(output_dir=output,
                  bands=bands,
                  warpMemoryLimit=warpmemorylimit,
                  clobber=clobber,
                  reproject=reproject)

    if jobs == 0:
        serial_process(hdf_files, **kwargs)
    else:
        p = multiprocessing.Pool(jobs)
        p.map(process_file, zip(hdf_files, [kwargs] * len(hdf_files)))

if __name__ == "__main__":
    main()
